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

import org.scalatest._
import pro.foundev.commons.test_support._
import pro.foundev.commons.benchmarking._

class MaxBenchmarkLauncherSpec extends BenchmarkSupport {

  override def beforeEach {
    super.beforeEach()
    benchmarkLauncher = new MaxBenchmarkLauncher(sc, tableSuffix)
    benchmarkLauncher.timer = timer
  }

  "A MaxBenchmarkLauncher" should "get a max value" in {
    benchmarkLauncher.all.value should be (10)
  }
  it should "have the name of max" in {
    benchmarkLauncher.all.name should be ("max")

  }
  it should "time the result" in {
    timer.setDuration(2000)
    benchmarkLauncher.all.milliSeconds should be (0.002)
  }
  "A MaxBenchmarkLauncher" should "get a sqlMax value" in {
    benchmarkLauncher.sqlAll.value should be (10)
  }
  it should "have the name of sqlMax" in {
    benchmarkLauncher.sqlAll.name should be ("sqlMax")
  }
  it should "time the result of sqlMax" in {
    timer.setDuration(2000)
    benchmarkLauncher.sqlAll.milliSeconds should be (0.002)
  }
  "A MaxBenchmarkLauncher" should "get an abbreviatedMax value" in {
    //this matches the first value inserted
    benchmarkLauncher.one.value should be (1)
  }
  it should "have the name of abbreviatedMax" in {
    benchmarkLauncher.one.name should be ("abbreviatedMax")
  }
  it should "time the result of abbreviatedMax" in {
    timer.setDuration(2000)
    benchmarkLauncher.one.milliSeconds should be (0.002)
  }
}
