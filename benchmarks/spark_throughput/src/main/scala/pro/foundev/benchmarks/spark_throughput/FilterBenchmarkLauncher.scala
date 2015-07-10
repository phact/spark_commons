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
import org.apache.spark.sql.SchemaRDD
import org.apache.spark.sql.cassandra.CassandraSQLContext

/**
 *
 * @param sc initialized Spark Context. This is needed to perform operations
 * @param tableSuffix the convention here is a table will run against different record counts.
 *                    So spark_test.records_1b in this case the tableSuffix would be "1b"
 */
class FilterBenchmarkLauncher(sc:SparkContext, tableSuffix: String)
  extends BenchmarkLauncher(sc, tableSuffix) {

  /**
   * Spark implementation of RDD.filter matches everything intentionally. Should return all results
   * @return should be result of benchmark run
   */
  override def all():Result={
    val filterCount = timer.profile(()=>{
        cassandraRDD
          .filter(x=>true)
          .count()
    })
    new Result("filter", timer.getMillis(), filterCount, tableSuffix)
  }

  /**
   * SQL query that matches always, returns all results
   * @return should be result of benchmark run
   */
  override def sqlAll():Result={
    timer.profile(()=>{
      val rdd: SchemaRDD = new CassandraSQLContext(sc)
        .sql("SELECT c0 from "+keyspace+"."+table + tableSuffix + " WHERE 1=1")
      rdd.count()
    })
    new Result("sqlFilter", timer.getMillis(), 1, tableSuffix)
  }
}
