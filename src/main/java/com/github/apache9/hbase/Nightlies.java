/*
 * Copyright 2024 Duo Zhang <palomino219@gmail.com>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.apache9.hbase;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.net.Authenticator;
import java.net.PasswordAuthentication;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;

public class Nightlies implements Closeable {

  private static final String NIGHTLIES_BASE_URL = "https://nightlies.apache.org/hbase/";

  private static final String USERNAME = System.getenv("APACHE_NIGHTLIES_USERNAME");

  private static final char[] PASSWORD = System.getenv("APACHE_NIGHTLIES_PASSWORD").toCharArray();

  private final HttpClient client =
      HttpClient.newBuilder()
          .authenticator(
              new Authenticator() {
                @Override
                protected PasswordAuthentication getPasswordAuthentication() {
                  return new PasswordAuthentication(USERNAME, PASSWORD);
                }
              })
          .followRedirects(HttpClient.Redirect.NORMAL)
          .build();

  private List<String> parse(HttpResponse<InputStream> resp) throws Exception {
    Document doc = Jsoup.parse(resp.body(), StandardCharsets.UTF_8.name(), "");
    List<String> list = new ArrayList<>();
    for (Element a : doc.selectXpath("//a")) {
      String href = a.attr("href");
      String text = a.text();
      if (href.equals(text)) {
        list.add(text);
      }
    }
    return list;
  }

  public List<String> list(String dir) throws Exception {
    URI url = new URI(NIGHTLIES_BASE_URL + dir);
    System.out.println("Listing " + url);
    HttpRequest req = HttpRequest.newBuilder(url).GET().build();
    HttpResponse<InputStream> resp = client.send(req, HttpResponse.BodyHandlers.ofInputStream());
    if (resp.statusCode() == 404) {
      return null;
    }
    if (resp.statusCode() == 200) {
      return parse(resp);
    }
    throw new IOException("Unexpected status code " + resp.statusCode() + " when listing " + dir);
  }

  public void delete(String dir) throws Exception {
    URI url = new URI(NIGHTLIES_BASE_URL + dir);
    System.out.println("Deleting " + url);
    HttpRequest req = HttpRequest.newBuilder(url).DELETE().build();
    HttpResponse<String> resp = client.send(req, HttpResponse.BodyHandlers.ofString());
    int code = resp.statusCode();
    if ((code < 200 || code >= 300) && resp.statusCode() != 404) {
      throw new IOException(
          "Unexpected status code " + resp.statusCode() + " when deleting " + dir);
    }
  }

  @Override
  public void close() {
    client.close();
  }
}
