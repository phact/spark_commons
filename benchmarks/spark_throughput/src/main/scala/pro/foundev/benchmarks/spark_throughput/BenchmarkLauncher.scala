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

import com.datastax.spark.connector.rdd._
import com.datastax.spark.connector._
import org.apache.spark.SparkContext
import pro.foundev.commons.benchmarking.SystemTimer
import pro.foundev.commons.benchmarking.Timer

/***
 * base class for benchmark launchers. subclass this to create a new benchmark calculation
 **/
abstract class BenchmarkLauncher(sc:SparkContext, tableSuffix: String) {
  var timer: Timer = new SystemTimer()
  val keyspace = "spark_test"
  val table = "records_"
  val cassandraRDD = sc.cassandraTable(keyspace, table+tableSuffix)

  def warmUp():Unit = {
    one()
    all()
    sqlAll()
  }
  def one():Result
  def all():Result
  def sqlAll():Result
}


