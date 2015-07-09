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
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SchemaRDD
import org.apache.spark.sql.cassandra.CassandraSQLContext
import pro.foundev.commons.benchmarking._

/**
 * for now doing silly count to do local memory operation to fire the union op
 **/
class UnionBenchmarkLauncher(sc:SparkContext, tableSuffix: String)
  extends BenchmarkLauncher(sc, tableSuffix) {

  override def all():Result={
    val unionCount = timer.profile(()=>{
      cassandraPairRDD.union(cassandraPairRDD)
      .count()
    })
    new Result("union", timer.getMillis(), unionCount, tableSuffix)
  }

  override def sqlAll():Result={
    val unionCount  = timer.profile(()=>{
      val rdd: SchemaRDD = new CassandraSQLContext(sc)
        .sql("SELECT value from "+keyspace+"."+table+ tableSuffix + " UNION "+
        "SELECT value from "+keyspace+"."+table+ tableSuffix)
      rdd.count()
    })
    new Result("sqlUnion", timer.getMillis(), unionCount, tableSuffix)
  }
}