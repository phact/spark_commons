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

import com.datastax.spark.connector.rdd._
import com.datastax.spark.connector._
import com.datastax.bdp.spark.DseSparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SchemaRDD
import org.apache.spark.sql.cassandra.CassandraSQLContext
import pro.foundev.commons.benchmarking._

/**
 * replicating groupBy logic,
 * SQL is for groupBy versus reduce is identical so there is nothing to do, just a no-op
 **/
class ReduceBenchmarkLauncher(sc:SparkContext, tableSuffix: String)
  extends BenchmarkLauncher(sc, tableSuffix) {

  override def all():Result={
    val groupByCount = timer.profile(()=>{
        cassandraPairRDD
        .reduceByKey(_ + _)
        .count()
    })
    new Result("reduceByKey", timer.getMillis(), groupByCount, tableSuffix)
  }

  override def sqlAll():Result={
    new Result("reduceBy not available", 0,0, tableSuffix)
  }
}
