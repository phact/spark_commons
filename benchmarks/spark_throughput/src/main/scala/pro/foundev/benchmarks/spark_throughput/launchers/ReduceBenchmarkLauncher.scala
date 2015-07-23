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
 * Reduce benchmark. should show favorably against groupBy
 * @param sc initialized Spark Context. This is needed to perform operations
 * @param tableSuffix the convention here is a table will run against different record counts.
 *                    So spark_test.records_1b in this case the tableSuffix would be "1b"
 */
class ReduceBenchmarkLauncher(sc:SparkContext, tableSuffix: String)
  extends BenchmarkLauncher(sc, tableSuffix) {

  /**
   * Standard reduce by key
   * @return should be result of benchmark run
   */
  override def all():Seq[Result]={
    val groupByCount = timer.profile(()=>{
        cassandraPairRDD
        .reduceByKey(_ + _)
          .count()
    })
    Seq(new Result("reduceByKey", timer.getMillis(), groupByCount, tableSuffix))
  }

  /**
   * No equivalent for reduce in SQL
   * @return should be result of benchmark run
   */
  override def sqlAll():Seq[Result]={
    Seq(new Result("reduceBy not available", 0,0, tableSuffix))
  }
}
