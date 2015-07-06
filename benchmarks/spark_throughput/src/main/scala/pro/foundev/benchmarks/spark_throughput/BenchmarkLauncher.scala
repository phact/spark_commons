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
    val tableSuffixes = Array("10k", "100k", "10m", "1b")
    val benches = tableSuffixes.map(s=>new MaxBenchmarkLauncher(sc, s))
    new BenchmarkRun(benches).exec()
  }
}

case class Result(name:String, milliSeconds: Double, value: Int, records: String){}

class BenchmarkRun(benches: Seq[BenchmarkLauncher], printer: PrintService=new StdPrintService()){

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

trait BenchmarkLauncher{
  def warmUp():Unit = {
    one()
    all()
    sqlAll()
  }

  def one():Result
  def all():Result
  def sqlAll():Result
}

class MaxBenchmarkLauncher(sc:SparkContext, tableSuffix: String, timer: Timer = new SystemTimer()) extends BenchmarkLauncher {

  val keyspace = "spark_test"
  val table = "records_"

  override def one():Result ={
    val max = timer.profile(()=>{
      cassandraValues()
        .take(1)
      .reduce((v1,v2)=>if(v1>v2){v1}; else{v2})
    })
    new Result("abbreviatedMax", timer.getMillis(), max, tableSuffix)
  }

  override def all():Result={
    val max = timer.profile(()=>{
      cassandraValues()
      .reduce((v1,v2)=>if(v1>v2){v1}; else{v2})
    })
    new Result("max", timer.getMillis(), max, tableSuffix)
  }

  override def sqlAll():Result={
    val max = timer.profile(()=>{
      val rdd: SchemaRDD = new CassandraSQLContext(sc)
        .sql("SELECT MAX(value) from "+keyspace+"."+table+ tableSuffix)
      rdd.take(1)(0).getInt(0)
    })
    new Result("sqlMax", timer.getMillis(), max, tableSuffix)
  }


  private def cassandraValues(): RDD[Int] = {
    sc.cassandraTable(keyspace, table+tableSuffix)
      .map(row=>row.getInt(1))
  }
}
