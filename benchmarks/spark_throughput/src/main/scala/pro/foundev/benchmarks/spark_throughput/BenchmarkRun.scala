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
import pro.foundev.commons.benchmarking.PrintService

/**
 * Main class for the benchmark program
 * @param benches Implemented benchmarks to run
 * @param printer output strategy. TODO: Consider refactoring to accepting Result objects for later analysis
 */
class BenchmarkRun(benches: Seq[BenchmarkLauncher], printer: PrintService){

  /**
   * Warms up all benchmarks, then runs the all method on each
   * and finishes with the sql equivalent where applicable
   */
  def exec(): Unit = {
    benches.foreach(b=>b.warmUp())
    benches.foreach(b=>logResults(b.all))
  //  benches.foreach(b=>logResults(b.sqlAll))
  }

  private def logResults(f:()=>Seq[Result]):Unit = {
    val fr = f()
    fr.foreach((results)=>
    printer.println(Array(results.name, results.milliSeconds, results.records).mkString(",")))
  }
}


