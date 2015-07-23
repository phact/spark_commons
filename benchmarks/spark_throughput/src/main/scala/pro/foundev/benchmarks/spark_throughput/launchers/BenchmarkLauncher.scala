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

package pro.foundev.benchmarks.spark_throughput.launchers

import com.datastax.spark.connector._
import org.apache.spark.SparkContext
import pro.foundev.benchmarks.spark_throughput.Result
import pro.foundev.commons.benchmarking.{SystemTimer, Timer}

/**
 * Base class for benchmark launchers. Subclass this to create a new benchmark type.
 * @param sc initialized Spark Context. This is needed to perform operations
 * @param tableSuffix the convention here is a table will run against different record counts.
 *                    So spark_test.records_1b in this case the tableSuffix would be "1b"
 */
abstract class BenchmarkLauncher(sc:SparkContext, tableSuffix: String) {
  var timer: Timer = new SystemTimer()
  val keyspace = "spark_test"
  val table = "records_"
  val fullTable = keyspace + "." + table + tableSuffix
  val cassandraRDD = sc.cassandraTable(keyspace, table+tableSuffix)
//  val cassandraPairRDD = cassandraRDD.select("id", "value") //doesn't work in 1.2
  val cassandraPairRDD = cassandraRDD.map(x=>(x.getUUID(0), x.getLong(1)))
  //TODO: Move more common functions down here

  /**
   * runs all and sqlAll to get all serialization and caching out of the way should not be profiled
   */
  def warmUp():Unit = {
    all()
   // sqlAll()
  }

  /**
   * Spark code benchmark
   * @return should be result of benchmark run
   */
  def all():Seq[Result]

  /**
   * Spark Sql code benchmark
   * @return should be result of benchmark run
   */
  def sqlAll():Seq[Result]

}
