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

import pro.foundev.benchmarks.spark_throughput.launchers.MaxBenchmarkLauncher

class MaxBenchmarkLauncherSpec extends BenchmarkSupport {

  override def beforeEach {
    super.beforeEach()
    benchmarkLauncher = new MaxBenchmarkLauncher(sc, tableSuffix)
    benchmarkLauncher.timer = timer
  }

  "A MaxBenchmarkLauncher" should "get a max value" in {
    benchmarkLauncher.all()(0).value should be (10L)
  }
  it should "have the name of max" in {
    benchmarkLauncher.all()(0).name should be ("max")

  }
  it should "time the result" in {
    timer.setDuration(2000)
    benchmarkLauncher.all()(0).milliSeconds should be (0.002)
  }
  "A MaxBenchmarkLauncher" should "get a sqlMax value" in {
    benchmarkLauncher.sqlAll()(0).value should be (10L)
  }
  it should "have the name of sqlMax" in {
    benchmarkLauncher.sqlAll()(0).name should be ("sqlMax")
  }
  it should "time the result of sqlMax" in {
    timer.setDuration(2000)
    benchmarkLauncher.sqlAll()(0).milliSeconds should be (0.002)
  }

}
