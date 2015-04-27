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
package pro.foundev

import pro.foundev.calculations.{LogCalculator, LogCalculatorImpl}
import pro.foundev.ingest.Loader
import pro.foundev.random.BenchmarkSeeding
import pro.foundev.reporting.RunTimeOptions
import pro.foundev.strategies.{JobSubmissionStrategy, BenchmarkStrategy, SparkStreamingStrategy}

object BenchmarkLauncher extends App{
  override def main(args: Array[String]) = {
    new BenchmarkLauncher().exec(args)
    println("done")
  }

}
class BenchmarkLauncher {
  def runTimeOptions(args: Array[String]):RunTimeOptions = {
    var master: String = "127.0.0.1"
    if (args.length > 0) {
      master = args(0)
    }
    var runs: Int = 100
    if (args.length > 1) {
      runs = args(1).toInt
    }
    var rowsToInsert: Long = 10000
    if (args.length > 2) {
      rowsToInsert = args(2).toLong
    }
    val enableLauncher = (args.length > 3 && (args(3) == "enableLoader"))
    new RunTimeOptions(master, runs,rowsToInsert ,enableLauncher)
  }

  def exec(args: Array[String]): Unit ={
    val options = this.runTimeOptions(args)
    val master = options.host
    val rowsToInsert = options.recordsToIngest
    val runs = options.runs

    if(options.enableLauncher) {
      val loader: Loader = new Loader(master)
      loader.create(rowsToInsert)
    }
    val calc: LogCalculator = new LogCalculatorImpl()
    val benchmarkSeeding: BenchmarkSeeding = new BenchmarkSeeding(rowsToInsert)

    val sparkStreamingStrategy: SparkStreamingStrategy = new SparkStreamingStrategy(master, calc,
      benchmarkSeeding)
    val sparkStreamingReport = sparkStreamingStrategy.run(runs)
    sparkStreamingStrategy.close()
    sparkStreamingStrategy.shutDown()

    val jobSubmissionStrategy: BenchmarkStrategy = new JobSubmissionStrategy(master, rowsToInsert, calc, benchmarkSeeding)
    val jobSubmissionReport = jobSubmissionStrategy.run(runs)
    println(jobSubmissionReport.generateFinalReport)

    /*
        val sparkJobServerStrategy: SparkJobServerStrategy = new SparkJobServerStrategy(master)
        val sparkJobServerReport = sparkJobServerStrategy.run(runs)

        val sparkSqlJDBCServerStrategy: SparkSqlJDBCServer = new SparkSqlJDBCServer(master)
        val sparkSqlReport =sparkSqlJDBCServerStrategy.run(runs)

        println(sparkJobServerReport.generateFinalReport)
        println(sparkSqlReport.generateFinalReport)
        */
    println(sparkStreamingReport.generateFinalReport)
  }
}
