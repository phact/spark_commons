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

import pro.foundev.benchmarks.spark_throughput.launchers.MinBenchmarkLauncher

class MinBenchmarkLauncherSpec extends BenchmarkSupport {

  override def beforeEach {
    super.beforeEach()
    benchmarkLauncherMin10k = new MinBenchmarkLauncher(sc, tableSuffix)
    benchmarkLauncherMin10k.timer = timer
  }

  "A MinBenchmarkLauncher" should "get a min value" in {
    benchmarkLauncherMin10k.all()(0).value should be (1l)
  }
  it should "have the name of min" in {
    benchmarkLauncherMin10k.all()(0).name should be ("min")
  }
  it should "time the result" in {
    timer.setDuration(2000)
    benchmarkLauncherMin10k.all()(0).milliSeconds should be (0.002)
  }
  "A MinBenchmarkLauncher" should "get a sqlMin value" in {
    benchmarkLauncherMin10k.sqlAll()(0).value should be (1l)
  }
  it should "have the name of sqlMin" in {
    benchmarkLauncherMin10k.sqlAll()(0).name should be ("sqlMin")
  }
  it should "time the result of sqlMin" in {
    timer.setDuration(2000)
    benchmarkLauncherMin10k.sqlAll()(0).milliSeconds should be (0.002)
  }
 }
