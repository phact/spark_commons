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

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SchemaRDD
import org.apache.spark.sql.cassandra.CassandraSQLContext
import pro.foundev.benchmarks.spark_throughput.Result

/**
 * Min benchmark. Should be the same as max.
 * @param sc initialized Spark Context. This is needed to perform operations
 * @param tableSuffix the convention here is a table will run against different record counts.
 *                    So spark_test.records_1b in this case the tableSuffix would be "1b"
 */
class MinBenchmarkLauncher(sc:SparkContext, tableSuffix: String)
  extends BenchmarkLauncher(sc, tableSuffix) {

  /**
   * Spark RDD.max implementation
   * @return should be result of benchmark run
   */
  override def all():Seq[Result]={
    val max = timer.profile(()=>{
      cassandraValues()
      .reduce((v1,v2)=>if(v1<v2){v1}; else{v2})
    })
    Seq(new Result("min", timer.getMillis(), max, tableSuffix))
  }

  /**
   * Spark SQL version of min
   * @return should be result of benchmark run
   */
  override def sqlAll():Seq[Result]={
    val max = timer.profile(()=>{
      val rdd: SchemaRDD = new CassandraSQLContext(sc)
        .sql("SELECT MIN(c0) from "+keyspace+"."+table+ tableSuffix)
      rdd.collect()
    })(0)(0).toString.toLong
    Seq(new Result("sqlMin", timer.getMillis(), max, tableSuffix))
  }
  private def cassandraValues(): RDD[Long] = {
    cassandraRDD
      .map(row=>row.getLong(1))
  }
}



