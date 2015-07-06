
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
import com.datastax.spark.connector.rdd._
import pro.foundev.commons.benchmarking._
import pro.foundev.commons.test_support._

class BenchmarkSupport extends CommonsTestSupport {
  this: Suite =>
  var timer: MockTimer = _
  var benchmarkLauncher: BenchmarkLauncher = _
  var benchmarkLauncherMin10k:BenchmarkLauncher = _
  val tableSuffix = "10k"

  def addRecords(tableSuffix: String):Unit = {
    cql("INSERT INTO spark_test.records_"+tableSuffix+" (key, value) values (1, 5)")
    cql("INSERT INTO spark_test.records_"+tableSuffix+" (key, value) values (2, 10)")
    cql("INSERT INTO spark_test.records_"+tableSuffix+" (key, value) values (3, 1)")
  }

  def createTableWithSuffix(tableSuffix:String):Unit = {
    makeTable("spark_test.records_"+tableSuffix, Seq(("key","int"), ("value","int")), "key")
  }

  override def beforeEach {
    super.beforeEach()
    timer = new MockTimer()
    makeKeyspace("spark_test")
    createTableWithSuffix(tableSuffix)
    addRecords(tableSuffix)
  }

}
