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

import java.util.Comparator;
import java.util.List;

public class PipelineJobCleaner {

  private final String jobName;

  private final int retain;

  private final Nightlies nightlies;

  public PipelineJobCleaner(String jobName, int retain, Nightlies nightlies) {
    this.jobName = jobName;
    this.retain = retain;
    this.nightlies = nightlies;
  }

  public void exec() throws Exception {
    System.out.println("Going to clean " + jobName);
    List<String> subDirs = nightlies.list(jobName + "/");
    for (String subDir : subDirs) {
      String toCleanDir = jobName + "/" + subDir;
      List<String> builds = nightlies.list(toCleanDir);
      // trim the last '/' before parsing
      builds.sort(Comparator.comparingInt(b -> Integer.parseInt(b.substring(0, b.length() - 1))));
      int toDeleteCount = builds.size() - retain;
      if (toDeleteCount > 0) {
        System.out.println(
            "Going to delete "
                + toDeleteCount
                + " builds from "
                + toCleanDir
                + " since it contains "
                + builds.size()
                + " builds, which is greater than threshold "
                + retain);
      } else {
        System.out.println(
            "Skip deleting builds from "
                + toCleanDir
                + " since it only contains "
                + builds.size()
                + " builds, which is less than threshold "
                + retain);
      }
      for (int i = 0; i < builds.size() - retain; i++) {
        nightlies.delete(toCleanDir + builds.get(i));
      }
    }
  }
}
