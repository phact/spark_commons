/*
 * Copyright 2015 Foundational Development
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

package pro.foundev.benchmarks.spark_throughput

class CogroupBenchmarkLauncherSpec extends BenchmarkSupport {

  override def beforeEach {
    super.beforeEach()
    benchmarkLauncher = new CogroupBenchmarkLauncher(sc, tableSuffix)
    benchmarkLauncher.timer = timer
  }

  "A CogroupBenchmarkLauncher" should "get a cogroup count" in {
    benchmarkLauncher.all.value should be (3l)
  }
  it should "have the name of cogroup" in {
    benchmarkLauncher.all.name should be ("cogroup")

  }
  it should "time the result" in {
    timer.setDuration(2000)
    benchmarkLauncher.all.milliSeconds should be (0.002)
  }
  /** sql co group proved too hard
  "A CogroupBenchmarkLauncher" should "get a sql cogroup count" in {
    benchmarkLauncher.sqlAll.value should be (3l)
  }
  it should "have the name of sqlCogroup" in {
    benchmarkLauncher.sqlAll.name should be ("sqlCogroup")
  }
  it should "time the result of sqlCogroup" in {
    timer.setDuration(2000)
    benchmarkLauncher.sqlAll.milliSeconds should be (0.002)
  }
  */

}
