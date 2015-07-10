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
 * Intersection benchmark
 * @param sc initialized Spark Context. This is needed to perform operations
 * @param tableSuffix the convention here is a table will run against different record counts.
 *                    So spark_test.records_1b in this case the tableSuffix would be "1b"
 */
class IntersectBenchmarkLauncher(sc:SparkContext, tableSuffix: String)
  extends BenchmarkLauncher(sc, tableSuffix) {

  /**
   * Intersection executed on pairRDD. Note: Purpose of count is to execute results of intersection
   * Performs intersection on the same table twice
   * @return should be result of benchmark run
   */
  override def all():Result={
    val intersectCount = timer.profile(()=>{
        cassandraPairRDD
        .intersection(cassandraPairRDD)
        .count()
    })
    new Result("intersect", timer.getMillis(), intersectCount, tableSuffix)
  }

  /**
   * Intersection using Spark SQL. Note: Purpose of count is to execute results of intersection
   * Performs intersection on the same table twice
   * @return should be result of benchmark run
   */
  override def sqlAll():Result={
    val intersect  = timer.profile(()=>{
      val rdd: SchemaRDD = new CassandraSQLContext(sc)
        .sql("SELECT * from "+keyspace+"."+table+ tableSuffix + " INTERSECT" +
        " SELECT * from "+keyspace+"."+table+ tableSuffix)
      rdd.count()
    })
    new Result("sqlIntersect", timer.getMillis(), intersect, tableSuffix)
  }
}
