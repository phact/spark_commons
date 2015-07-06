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

import com.datastax.bdp.spark.DseSparkContext
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import pro.foundev.commons.benchmarking._

object BenchmarkRun {
  def main(args: Array[String])={
    val sc: SparkContext = DseSparkContext(new SparkConf()
      .set("driver.host", args(0))
      .setAppName("spark throughput")
      .set("spark.eventLog.enabled", "true")
      //necessary to set jar for api submission
      .setJars(Array("spark_throughput-assembly.jar"))
      .setMaster(args(1))
    )
    val printer = new StdPrintService()
    val tableSuffixes = Array("10k", "100k", "10m", "1b")
    val maxBenches = tableSuffixes.map(s=>new MaxBenchmarkLauncher(sc, s))
    new BenchmarkRun(maxBenches, printer).exec()
    val minBenches = tableSuffixes.map(s=>new MinBenchmarkLauncher(sc, s))
    new BenchmarkRun(minBenches, printer).exec()
  }
}

class BenchmarkRun(benches: Seq[BenchmarkLauncher], printer: PrintService){

  def exec(): Unit = {
    printer.println("start benchmarks")
    benches.foreach(b=>b.warmUp())
    benches.foreach(b=>logResults(b.one))
    benches.foreach(b=>logResults(b.all))
    benches.foreach(b=>logResults(b.sqlAll))
    printer.println("benchmark done")
  }

  private def logResults(f:()=>Result):Unit = {
    val results = f()
    printer.println(results.milliSeconds + " milliseconds to run " + results.name + " on " + results.records  +" records")
  }
}


