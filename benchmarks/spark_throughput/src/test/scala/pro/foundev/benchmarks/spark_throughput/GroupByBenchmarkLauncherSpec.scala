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

import pro.foundev.benchmarks.spark_throughput.launchers.GroupByBenchmarkLauncher

class GroupByBenchmarkLauncherSpec extends BenchmarkSupport {

  override def beforeEach {
    super.beforeEach()
    benchmarkLauncher = new GroupByBenchmarkLauncher(sc, tableSuffix)
    benchmarkLauncher.timer = timer
  }

  "A GroupByBenchmarkLauncher" should "get a groupBy count" in {
    benchmarkLauncher.all()(0).value should be (3l)
  }
  it should "have the name of groupBy" in {
    benchmarkLauncher.all()(0).name should be ("groupBy")

  }
  it should "time the result" in {
    timer.setDuration(2000)
    benchmarkLauncher.all()(0).milliSeconds should be (0.002)
  }
  "A GroupByBenchmarkLauncher with sql " should "get a sql groupBy count" in {
    benchmarkLauncher.sqlAll()(0).value should be (3l)
  }
  it should "have the name of sqlGroupBy" in {
    benchmarkLauncher.sqlAll()(0).name should be ("sqlGroupBy")
  }
  it should "time the result of sqlGroupBy" in {
    timer.setDuration(2000)
    benchmarkLauncher.sqlAll()(0).milliSeconds should be (0.002)
  }

}
