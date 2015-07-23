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

import pro.foundev.benchmarks.spark_throughput.launchers.BenchmarkLauncher

import scala.collection.mutable._

class BenchmarkLauncherSpec extends BenchmarkSupport {

  "A BenchmarkLauncher " should "run all methods on warmup" in  {
    val allSpy = ArrayBuffer.empty[Long]
    val sqlSpy = ArrayBuffer.empty[Long]
    val launcher = new BenchmarkLauncher(sc, "10k") {
      override def all(): Seq[Result] = {
        allSpy += 2
        null
      }

      override def sqlAll(): Seq[Result] = {
        sqlSpy += 3
        null
      }
    }
    launcher.warmUp()
    sqlSpy.size should be (1)
    allSpy.size should be (1)
  }
}

