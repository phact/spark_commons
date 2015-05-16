/*
 *  Copyright 2015 Foundational Development
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 *
 */
package pro.foundev.strategies

import com.datastax.bdp.spark.DseSparkContext
import com.datastax.spark.connector.cql.CassandraConnector
import org.apache.spark.{SparkConf, SparkContext}
import pro.foundev.calculations.{CheapLogCalculator, LogCalculator}
import pro.foundev.cassandra.CassandraRepository
import pro.foundev.random.BenchmarkSeeding

class JobSubmissionStrategy(master: String,
                            recordCount: Long,
                            calculator: LogCalculator,
                             benchmarkSeeding: BenchmarkSeeding) extends BenchmarkStrategy(master){
  var repository = new CassandraRepository()


  private def createContext: SparkContext = {
    //DseSparkContext is include with the dse-4.7.x.jar
     DseSparkContext(new SparkConf()
      .set("driver.host", master)
      .setAppName("My application")
      .set("spark.cassandra.output.concurrent.writes","1")
      .set("spark.cassandra.output.batch.size.rows", "1")
      .set("spark.cassandra.input.split.size", "10000")
      .set("spark.cassandra.input.page.row.size", "10")
      //necessary to set jar for api submission
      .setJars(Array("target/scala-2.10/interactive_spark_benchmarks-assembly.jar"))
      .setMaster(getMasterUrl())
      )
  }

  override protected def getStrategyName: String = { "API Job Submission Strategy" }

  override protected def executeComputationOnRangeOfPartitions(): Unit = {

    val sparkContext: SparkContext = createContext
    val logs = repository.getLogsForIds(benchmarkSeeding.ids,CassandraConnector(sparkContext.getConf))
    val rdd = sparkContext.parallelize(logs)
    calculator.sessionReport(rdd)
    sparkContext.stop
  }

  override protected def executeComputationOnPartitionFoundOn2i(): Unit = {
    val sparkContext: SparkContext = createContext
    val state = benchmarkSeeding.state
    val rdd = repository.getLogsFromRandom2i(state, sparkContext)
    calculator.sessionReport(rdd)
    sparkContext.stop
  }


  override protected def executeCheapComputationOnSinglePartition(): Unit = {
    val sparkContext: SparkContext = createContext
    val log = repository.getLogForId(benchmarkSeeding.randId, CassandraConnector(sparkContext.getConf))
    val rdd = sparkContext.parallelize(List(log))
    new CheapLogCalculator().totalUrls(rdd)
    sparkContext.stop
  }
}
