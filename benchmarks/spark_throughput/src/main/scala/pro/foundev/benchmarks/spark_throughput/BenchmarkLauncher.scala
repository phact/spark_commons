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
import com.datastax.bdp.spark.DseSparkContext
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SchemaRDD
import org.apache.spark.sql.cassandra.CassandraSQLContext
import pro.foundev.commons.benchmarking._

object BenchmarkLauncher {
  def main(args: Array[String])={
         val sc: SparkContext = DseSparkContext(new SparkConf()
      .set("driver.host", args(0))
      .setAppName("spark throughput")
      //.set("spark.cassandra.output.concurrent.writes","1")
      //.set("spark.cassandra.output.batch.size.rows", "1")
      //.set("spark.cassandra.input.split.size", "10000")
      //.set("spark.cassandra.input.page.row.size", "10")
      .set("spark.eventLog.enabled", "true")
      //necessary to set jar for api submission
      .setJars(Array("spark_throughput-assembly.jar"))
      .setMaster(args(1))
    )
    val bench = new BenchmarkLauncher(sc)
    new BenchmarkRun(bench).exec()
  }

}
case class Result(name:String, milliSeconds: Double, value: Int){}
class BenchmarkRun(bench: BenchmarkLauncher, printer: PrintService=new StdPrintService()){

  def exec(): Unit = {
    printer.println("start benchmarks")
    bench.warmUp()
    logResults(bench.abbreviatedMax)
    logResults(bench.max)
    logResults(bench.sqlMax)
    printer.println("benchmark done")
  }

  private def logResults(f:()=>Result):Unit = {
    val results = f()
    printer.println(results.milliSeconds + " milliseconds to run " + results.name)
  }
}

class BenchmarkLauncher(sc:SparkContext, timer: Timer = new SystemTimer()) {
  val keyspace = "keyspace1"
  val table = "standard1"

  def warmUp():Unit = {
    max
    sqlMax
  }

  def abbreviatedMax():Result ={
    val max = timer.profile(()=>{
      cassandraValues()
        .take(1)
      .reduce((v1,v2)=>if(v1>v2){v1}; else{v2})
    })
    new Result("abbreviatedMax", getMillis(), max)
  }

  def max():Result={
    val max = timer.profile(()=>{
      cassandraValues()
      .reduce((v1,v2)=>if(v1>v2){v1}; else{v2})
    })
    new Result("max", getMillis(), max)
  }

  def sqlMax():Result={
    val max = timer.profile(()=>{
      val rdd: SchemaRDD = new CassandraSQLContext(sc).sql("SELECT MAX(value) from "+keyspace+"."+table)
      rdd.take(1)(0).getInt(0)
    })
    new Result("sqlMax", getMillis(), max)
  }

  private def getMillis():Double = {
    timer.lastProfile/1000000.0
  }

  private def cassandraValues(): RDD[Int] = {
    sc.cassandraTable(keyspace, table)
      .map(row=>row.getInt(1))
  }
}
