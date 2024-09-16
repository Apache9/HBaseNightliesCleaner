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

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import org.yaml.snakeyaml.Yaml;

public class Config {

  private List<PipelineConfig> pipeline;

  public List<PipelineConfig> getPipeline() {
    return pipeline;
  }

  public void setPipeline(List<PipelineConfig> pipeline) {
    this.pipeline = pipeline;
  }

  public static Config load(String file) throws IOException {
    Yaml yaml = new Yaml();
    String content = Files.readString(Paths.get(file), StandardCharsets.UTF_8);
    return yaml.loadAs(content, Config.class);
  }
}
