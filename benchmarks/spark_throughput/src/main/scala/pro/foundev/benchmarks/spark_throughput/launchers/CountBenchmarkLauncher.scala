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
import org.apache.spark.sql.SchemaRDD
import org.apache.spark.sql.cassandra.CassandraSQLContext
import pro.foundev.benchmarks.spark_throughput.Result

/**
 *
 * Count benchmark. Relies on the Spark Cassandra Connector count operations
 * for now doing silly count to do local memory operation to fire the groupBy
 * @param sc initialized Spark Context. This is needed to perform operations
 * @param tableSuffix the convention here is a table will run against different record counts.
 *                    So spark_test.records_1b in this case the tableSuffix would be "1b"
 */
class CountBenchmarkLauncher(sc:SparkContext, tableSuffix: String)
  extends BenchmarkLauncher(sc, tableSuffix) {

  /**
   * Does a simple count. This will underneath use the Spark Cassandra Connector's implementation of count and
   * not the traditional approach to counting that Spark uses
   * @return should be result of benchmark run
   */
  override def all():Seq[Result]={
    val count = timer.profile(()=>{
        cassandraRDD
        .count()
    })
    Seq(new Result("count", timer.getMillis(), count, tableSuffix))
  }

  /**
   *  This SHOULD behave the same way as CassandraTableScanRDD.count()
   *  TODO: verify underlying implementation
   * @return should be result of benchmark run
   */
  override def sqlAll():Seq[Result]={
    val count  = timer.profile(()=>{
      val rdd: SchemaRDD = new CassandraSQLContext(sc)
        .sql("SELECT COUNT(*) from "+keyspace+"."+table+ tableSuffix)
      rdd.count()
    })
    Seq(new Result("sqlCount", timer.getMillis(), count, tableSuffix))
  }
}
