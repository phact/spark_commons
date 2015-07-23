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
import org.apache.spark.SparkContext._
import org.apache.spark.sql.SchemaRDD
import org.apache.spark.sql.cassandra.CassandraSQLContext
import pro.foundev.benchmarks.spark_throughput.Result


/**
 * Group by implementation
 * @param sc initialized Spark Context. This is needed to perform operations
 * @param tableSuffix the convention here is a table will run against different record counts.
 *                    So spark_test.records_1b in this case the tableSuffix would be "1b"
 */
class GroupByBenchmarkLauncher(sc:SparkContext, tableSuffix: String)
  extends BenchmarkLauncher(sc, tableSuffix) {

  /**
   * Group by executed on pairRDD. Note: Purpose of count is to execute results of groupBy
   * @return should be result of benchmark run
   */
  override def all():Seq[Result]={
    val groupByCount = timer.profile(()=>{
        cassandraPairRDD
        .groupByKey()
        .count()
    })
    Seq(new Result("groupBy", timer.getMillis(), groupByCount, tableSuffix))
  }

  /**
   * Group by using Spark SQL. Note: Purpose of count is to execute results of groupBy
   * @return should be result of benchmark run
   */
  override def sqlAll():Seq[Result]={
    val groupByCount  = timer.profile(()=>{
      val rdd: SchemaRDD = new CassandraSQLContext(sc)
        .sql("SELECT c0 from "+keyspace+"."+table+ tableSuffix + " GROUP BY id, c0")
      rdd.count()
    })
    Seq(new Result("sqlGroupBy", timer.getMillis(), groupByCount, tableSuffix))
  }
}
