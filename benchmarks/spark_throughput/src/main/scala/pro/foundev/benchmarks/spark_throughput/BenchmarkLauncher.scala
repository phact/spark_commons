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

/***
 * 
 */
package pro.foundev.benchmarks.spark_throughput

import com.datastax.spark.connector.rdd._
import com.datastax.spark.connector._
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SchemaRDD
import org.apache.spark.sql.cassandra.CassandraSQLContext
import pro.foundev.commons.benchmarking.Timer
import pro.foundev.commons.benchmarking.SystemTimer

object BenchmarkLauncher {
  def main(args: Array[String])={
    println("start benchmarks")
    //"SELECT COUNT(*) FROM "
    println("benchmark done")
  }

}
case class Result(name:String, milliSeconds: Double, value: Int){}

class BenchmarkLauncher(sc:SparkContext, timer: Timer = new SystemTimer()) {
  val keyspace = "keyspace1"
  val table = "standard1"

  def warmUp():Unit = {
    max
    sqlMax
  }

  def abbreviatedMax():Result ={
    val rdd =  cassandraValues()
      .take(1)
      .reduce((v1,v2)=>if(v1>v2){v1}; else{v2})
    val max = rdd
    new Result("short max", 0.0, max)
  }

  def max():Result={
    val max = timer.profile(()=>{
      cassandraValues()
      .reduce((v1,v2)=>if(v1>v2){v1}; else{v2})
    })
    new Result("max", getMillis(), max)
  }

  def sqlMax():Result={
    val rdd: SchemaRDD = new CassandraSQLContext(sc).sql("SELECT MAX(c1) from "+keyspace+"."+table)
    val max: Int = rdd.take(1)(0).getInt(0)
    new Result("sql max", 0.0, max)
  }

  private def getMillis():Double = {
    timer.lastProfile/1000000.0
  }

  private def cassandraValues(): RDD[Int] = {
    sc.cassandraTable(keyspace, table)
      .map(row=>row.getInt(1))
  }
}
