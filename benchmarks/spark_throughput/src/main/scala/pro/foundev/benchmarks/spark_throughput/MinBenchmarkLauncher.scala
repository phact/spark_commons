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

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SchemaRDD
import org.apache.spark.sql.cassandra.CassandraSQLContext

class MinBenchmarkLauncher(sc:SparkContext, tableSuffix: String)
  extends BenchmarkLauncher(sc, tableSuffix) {

  override def one():Result ={
    val max = timer.profile(()=>{
      cassandraValues()
        .take(1)
      .reduce((v1,v2)=>if(v1<v2){v1}; else{v2})
    })
    new Result("abbreviatedMin", timer.getMillis(), max, tableSuffix)
  }

  override def all():Result={
    val max = timer.profile(()=>{
      cassandraValues()
      .reduce((v1,v2)=>if(v1<v2){v1}; else{v2})
    })
    new Result("min", timer.getMillis(), max, tableSuffix)
  }

  override def sqlAll():Result={
    val max = timer.profile(()=>{
      val rdd: SchemaRDD = new CassandraSQLContext(sc)
        .sql("SELECT MIN(value) from "+keyspace+"."+table+ tableSuffix)
      rdd.take(1)(0).getInt(0)
    })
    new Result("sqlMin", timer.getMillis(), max, tableSuffix)
  }
  private def cassandraValues(): RDD[Int] = {
    cassandraRDD
      .map(row=>row.getInt(1))
  }
}



