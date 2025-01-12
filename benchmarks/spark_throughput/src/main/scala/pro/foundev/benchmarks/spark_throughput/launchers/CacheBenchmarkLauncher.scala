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
import org.apache.spark.storage.StorageLevel
import pro.foundev.benchmarks.spark_throughput.Result

class CacheBenchmarkLauncher(sc: SparkContext, tableSuffix: String, st: StorageLevel )
  extends BenchmarkLauncher(sc, tableSuffix) {

  /**
   * Spark code benchmark
   * @return should be result of benchmark run
   */
  override def all(): Seq[Result] = {
    val cachedRDD = cassandraRDD.map(x=>x.getLong(1)).persist(st)
    timer.profile(()=>cachedRDD.max())
    val firstResult = new Result("firstRun " + st.description, timer.getMillis(), 0, tableSuffix)
    timer.profile(()=>cachedRDD.max())
    val secondResult = new Result("secondRun " + st.description, timer.getMillis(), 0, tableSuffix)
    cachedRDD.unpersist(true)
    Seq(firstResult, secondResult)
  }

  /**
   * Spark Sql code benchmark
   * @return should be result of benchmark run
   */
  override def sqlAll(): Seq[Result] = ???
}
