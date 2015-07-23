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
import pro.foundev.benchmarks.spark_throughput.launchers.{MaxBenchmarkLauncher, BenchmarkLauncher}
import pro.foundev.commons.test_support._
import pro.foundev.commons.benchmarking._
import scala.collection.mutable._

class BenchmarkRunSpec extends BenchmarkSupport {
  var mockPrint: MockPrint = _

  override def beforeEach {
    super.beforeEach()
    mockPrint = new MockPrint()
    timer.setDuration(2000000)
    benchmarkLauncher = new MaxBenchmarkLauncher(sc, "10k")
    benchmarkLauncher.timer = timer
    val benchmarkLauncher100k:BenchmarkLauncher = new MaxBenchmarkLauncher(sc, "100k")
    benchmarkLauncher100k.timer = timer
    createTableWithSuffix("100k")
    addRecords("100k")
    val launchers= Array(benchmarkLauncher, benchmarkLauncher100k)
    new BenchmarkRun(launchers, mockPrint).exec()
  }

  //TODO: hack to get this done quickly, move elsewhere
  "A BenchmarkRun" should "log results of benchmarks" in {
    mockPrint.messages(0) should be ("start benchmarks")
    mockPrint.messages(1) should be ("2.0 milliseconds to run max on 10k records")
    mockPrint.messages(2) should be ("2.0 milliseconds to run max on 100k records")
    mockPrint.messages(3) should be ("2.0 milliseconds to run sqlMax on 10k records")
    mockPrint.messages(4) should be ("2.0 milliseconds to run sqlMax on 100k records")
    mockPrint.messages(5) should be ("benchmark done")
  }
}
