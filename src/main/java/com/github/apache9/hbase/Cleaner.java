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
import java.util.ArrayList;
import java.util.List;

public class Cleaner implements Closeable {

  private final Nightlies nightlies;

  private final List<PipelineJobCleaner> cleaners;

  public Cleaner(String configFile) throws IOException {
    Config config = Config.load(configFile);
    nightlies = new Nightlies();
    cleaners = new ArrayList<>();
    for (PipelineConfig pipelineConfig : config.getPipeline()) {
      cleaners.add(
          new PipelineJobCleaner(
              pipelineConfig.getJobName(), pipelineConfig.getRetain(), nightlies));
    }
  }

  public void exec() throws Exception {
    for (PipelineJobCleaner cleaner : cleaners) {
      cleaner.exec();
    }
  }

  @Override
  public void close() {
    nightlies.close();
  }

  public static void main(String[] args) throws Exception {
    if (args.length < 1) {
      System.err.println("Usage: <executable> <config_file>");
      return;
    }
    try (Cleaner cleaner = new Cleaner(args[0])) {
      cleaner.exec();
    }
  }
}
