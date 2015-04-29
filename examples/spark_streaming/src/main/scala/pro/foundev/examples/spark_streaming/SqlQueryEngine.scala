/*
 *  Copyright 2015 Foundational Development
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 *
 */

package pro.foundev.examples.spark_streaming

import _root_.java.util.Date

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SchemaRDD
import org.apache.spark.sql.cassandra.CassandraSQLContext
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.SparkContext._
import pro.foundev.examples.spark_streaming.messaging.RabbitMqCapable
import pro.foundev.examples.spark_streaming.utils.Args

object SqlQueryEngine {
  def main(args: Array[String]) = {
    val master = Args.parseMaster(args)
    new SqlQueryEngine(master).startJob()
  }
}

class SqlQueryEngine(master: String) extends RabbitMqCapable(master, "spark_sql_query_engine"){
  override def createContext(): StreamingContext = {
    val (dstream, ssc, connector) = connectToExchange()
    val csc = new CassandraSQLContext(ssc.sparkContext)
    val temp = csc.sql("SELECT * FROM st.session_logs")
    temp.registerTempTable("logs")
    val schemaConvert = (rdd: RDD[(String, String)])=>
      rdd.map(message=>
      (message._1, csc.sql(message._2)
        .map(row=>row.toString())))

    dstream
      .map(x=>x.split(":"))
      .map(x=>(x(0), x(1)))
      .transform(x=>schemaConvert(x))
      .foreachRDD(x=>x.foreachPartition(p=>{
      while(p.hasNext){
        val results = p.next()
        println("response from query id " + results._1 )
        results._2.foreach(x=>println(x))
      }
    }))

    ssc
  }
}
