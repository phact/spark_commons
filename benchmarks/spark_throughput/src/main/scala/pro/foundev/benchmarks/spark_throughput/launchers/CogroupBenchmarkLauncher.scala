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
import pro.foundev.benchmarks.spark_throughput.Result


/**
 * Cogroup comparison. Note: for now doing silly
 * count to do local memory operation to fire the cogroup
 * @param sc initialized Spark Context. This is needed to perform operations
 * @param tableSuffix the convention here is a table will run against different record counts.
 *                    So spark_test.records_1b in this case the tableSuffix would be "1b"
 */
class CogroupBenchmarkLauncher(sc:SparkContext, tableSuffix: String)
  extends BenchmarkLauncher(sc, tableSuffix) {

  /**
   * cogroups on the requested cassandra table
   * @return should be result of benchmark run
   */
  override def all():Seq[Result]={
    val cogroupCount = timer.profile(()=>{
        cassandraPairRDD
        .cogroup(cassandraPairRDD)
        .count()
    })
    Seq(new Result("cogroup", timer.getMillis(), cogroupCount, tableSuffix))
  }

  /**
   *  sql equivalent ( I believe) to cogroup that joins a table on itself then groups by
   * @return should be result of benchmark run
   */
  override def sqlAll():Seq[Result]={
    /** sql co group proved too difficult to get correct
    val cogroupCount  = timer.profile(()=>{
      val rdd: SchemaRDD = new CassandraSQLContext(sc)
        .sql("SELECT a1.c0, a2.c0 from " + fullTable+ " a1 JOIN " + fullTable + " a2 on a1.id=a2.id GROUP BY a1.id, a1.c0, a2.c0")
      rdd.count()
    })
    */
    Seq(new Result("sqlCogroup N/A", 0,0, tableSuffix))
  }
}
