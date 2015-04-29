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

import _root_.java.text.SimpleDateFormat
import _root_.java.util.Date

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}
import pro.foundev.examples.spark_streaming.java.messaging.RabbitMQReceiver
import pro.foundev.examples.spark_streaming.messaging.RabbitMqCapable
import pro.foundev.examples.spark_streaming.utils.Args


object SqlQueryEngineOnWindows {
  def main(args: Array[String])  = {
    val master = Args.parseMaster(args)
    new SqlQueryEngineOnWindows(master).startJob()
  }
}

class SqlQueryEngineOnWindows(master: String)
  extends RabbitMqCapable(master, "sql_query_engine_on_windows"){
  override def createContext(): StreamingContext = {
    val (dstream, ssc, connector) = connectToExchange()
    val sqlContext = new SQLContext(ssc.sparkContext)
    val format = new SimpleDateFormat("yyyy-MM-dd")
    val queryStream = ssc
      .receiverStream(new RabbitMQReceiver(StorageLevel.MEMORY_AND_DISK_2, master, "queries"))
    import sqlContext.createSchemaRDD

    //every 10 seconds look at the past 60 seconds
    val transactions = dstream.window(Seconds(60), Seconds(10)).map(line=> {
      val columns = line.split(",")
      val taxId = columns(0)
      val name = columns(1)
      val merchant = columns(2)
      val amount = BigDecimal(columns(3))
      val transactionDate = format.parse(columns(4))
      println(line)
      (taxId, (name, merchant, amount, transactionDate))
    }).cache()

    queryStream
      .map(x=>x.split(":"))
      .map(x=>(x(0), x(1)))
      .foreachRDD(queryRDD=>{
      queryRDD.foreach(queryMessage=>{
        val queryId = queryMessage._1
        val query = queryMessage._2
        transactions.transform(x=> {
          x.registerTempTable("transactions")
          sqlContext.sql(query)
        }).print()
      })
     })
    ssc
  }
}
