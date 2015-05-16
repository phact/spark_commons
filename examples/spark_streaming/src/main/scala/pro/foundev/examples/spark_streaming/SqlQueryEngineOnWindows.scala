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

import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}
import pro.foundev.examples.spark_streaming.java.messaging.RabbitMQReceiver
import pro.foundev.examples.spark_streaming.messaging.RabbitMqCapable
import pro.foundev.examples.spark_streaming.utils.Args


case class Transaction(taxId: String, name: String, merchant:String, amount: Double, transactionDate: String )

/**
 *
 * Requirements
 *  - CFS must be available
 *  - DSE 4.7
 *  - rabbitmq 3.4.4 is up.
 *  - Runs on localhost and the external interface by default.
 *  - Also must connect to the external.
 *
 * How to Run
 *
 *  - checkout source
 *  - install sbt
 *  - sbt assembly
 *  - java -cp target/scala-2.10/dse_spark_streaming_examples-assembly.jar pro.foundev.examples.spark_streaming.java.messaging.RabbitMQEmitWarnings #connects to local host
 *  - dse spark-submit --class pro.foundev.examples.spark_streaming.SqlQueryEngineOnWindows dse_spark_streaming_examples-assembly.jar |ip_address|
 *  - rabbitmqadmin publish exchange=queries payload="1:SELECT COUNT(*) FROM transactions_over_past_minute" routing_key=null
 */
object SqlQueryEngineOnWindows {
  def main(args: Array[String])  = {
    val master = Args.parseMaster(args)
    new SqlQueryEngineOnWindows(master).startJob()
  }
}

/**
 * Relies on CFS being present
 * @param master node to connect to, this should probably be whatever rpc_address or rpc_broadcast is set to
 */
class SqlQueryEngineOnWindows(master: String)
  extends RabbitMqCapable(master, "sql_query_engine_on_windows", "SqlQueryEngineOnWindows") {
  override def createContext(): StreamingContext = {
    val (dstream, ssc, connector) = connectToExchange()
    val sqlContext = new SQLContext(ssc.sparkContext)
    val emptyRDD: RDD[Row] = ssc.sparkContext.emptyRDD

    val queryStream = ssc
      .receiverStream(new RabbitMQReceiver(StorageLevel.MEMORY_AND_DISK_2, master, "queries"))
    /**
     * runs every 5 seconds look at the past 60 seconds.
     * the required message format is taxId, name, merchant name, dollar amount, transactionDate
     * example 999-99-9999, John Smith, Staples, 120.34, 2015-01-30
     */
    val transactions = dstream.window(Seconds(60), Seconds(5)).map(line => {
      val columns = line.split(",")
      val taxId = columns(0)
      val name = columns(1)
      val merchant = columns(2)
      val amount = columns(3).toDouble
      val transactionDate = columns(4)
      println(line)
      new Transaction(taxId, name, merchant, amount, transactionDate)
    }).cache()
    /**
     * requires messages to be in the format of queryId:query
     * this assumes the query is on the transactions table
     * for example: 1:SELECT count(*) as trans_count from transactions_over_past_minute
     */
    val queries = queryStream.
      map(x=>x.split(":"))
      .cache()

    /**
     * Combines both dstreams to a given single stream
     */
    queries.transformWith(transactions, (queriesRDD, t: RDD[Transaction])=>{
      import sqlContext.createSchemaRDD
      t.registerTempTable("transactions_over_past_minute")
      val queries = queriesRDD.collect()
      if(queries.length>0){
        // just get first one for example. Would iterate through this with foreachRDD
        val queryMessage = queries(0)
        val id = queryMessage(0)
        val query = queryMessage(1)
        println("query id: "+id)
        println("query : " + query)
        sqlContext.sql(query).cache()
      }else{
        println("no queries to answer")
        emptyRDD
      }
    }).print

    ssc
  }
}
