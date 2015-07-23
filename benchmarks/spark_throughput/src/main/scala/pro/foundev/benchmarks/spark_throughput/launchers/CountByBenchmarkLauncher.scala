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
import org.apache.spark.sql.cassandra.CassandraSQLContext
import pro.foundev.benchmarks.spark_throughput.Result

/**
 * Will count by key, should always be 1
 * @param sc initialized Spark Context. This is needed to perform operations
 * @param tableSuffix the convention here is a table will run against different record counts.
 *                    So spark_test.records_1b in this case the tableSuffix would be "1b"
 */
class CountByBenchmarkLauncher(sc:SparkContext, tableSuffix: String)
  extends BenchmarkLauncher(sc, tableSuffix) {

  /**
   * countByKey implementation. The RDD.count op is moved outside timer as response will come back in countBy. TODO: Concerned
   * this may pollute benchmarking
   * @return should be result of benchmark run
   */
  override def all():Seq[Result]={
    val count = timer.profile(()=>{
        cassandraPairRDD
          .countByKey()
    })
    Seq(new Result("countBy", timer.getMillis(),0, tableSuffix))
  }

  /**
   * Spark SQL version of countByKey.
   * @return should be result of benchmark run
   */
  override def sqlAll():Seq[Result]={
    val countByCount  = timer.profile(()=> {
      new CassandraSQLContext(sc)
        .sql("SELECT COUNT(c0) from " + keyspace + "." + table + tableSuffix + " GROUP BY id")
      .count()
    })
    Seq(new Result("sqlCountBy", timer.getMillis(), countByCount, tableSuffix))
  }
}
