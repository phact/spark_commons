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
 * Simple benchmark of RDD.first
 * @param sc initialized Spark Context. This is needed to perform operations
 * @param tableSuffix the convention here is a table will run against different record counts.
 *                    So spark_test.records_1b in this case the tableSuffix would be "1b"
 */
class FirstBenchmarkLauncher(sc:SparkContext, tableSuffix: String)
  extends BenchmarkLauncher(sc, tableSuffix) {

  /**
   * Simple retrieval of first record using RDD.first()
   * @return should be result of benchmark run
   */
  override def all():Seq[Result]={
    timer.profile(()=>{
        cassandraRDD
        .first()
    })
    Seq(new Result("first", timer.getMillis(), 1, tableSuffix))
  }

  /**
   * SQL limit 1 should be equivalent to RDD.first()
   * @return should be result of benchmark run
   */
  override def sqlAll():Seq[Result]={
    timer.profile(()=>{
      val rdd: SchemaRDD = new CassandraSQLContext(sc)
        .sql("SELECT * from "+keyspace+"."+table+ tableSuffix + " LIMIT 1")
      rdd.count()
    })
    Seq(new Result("sqlFirst", timer.getMillis(), 1, tableSuffix))
  }
}
