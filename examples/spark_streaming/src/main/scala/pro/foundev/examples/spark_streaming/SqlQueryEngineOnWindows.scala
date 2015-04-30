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
object SqlQueryEngineOnWindows {
  def main(args: Array[String])  = {
    val master = Args.parseMaster(args)
    new SqlQueryEngineOnWindows(master).startJob()
  }
}

/**
 * - This spark job requires CFS to be in place and will place a sql_query_engine_on_windows
 * checkpoint directory at the base level of CFS.
 * - RabbitMQ dependencies include an exchange named warnings and another named queries.
 * - I've chosen to also have output go to the console instead of to a message queue. There are
 * other examples in the code base demonstrating the output to queue technique such as
 * pro.foundev.examples.spark_streaming.java.interactive.smartconsumer.ResponseWriter
 * @param master
 */
class SqlQueryEngineOnWindows(master: String)
  extends RabbitMqCapable(master, "sql_query_engine_on_windows") {
  override def createContext(): StreamingContext = {
    val (dstream, ssc, connector) = connectToExchange()
    val sqlContext = new SQLContext(ssc.sparkContext)
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
 //     Row(taxId, name, merchant, amount, transactionDate)
      new Transaction(taxId, name, merchant, amount, transactionDate)
    })
/*
      val schema = new StructType(Seq(
          StructField("taxId", StringType, true),
          StructField("name", StringType, true),
          StructField("merchant", StringType, true),
          StructField("amount", DoubleType, true),
          StructField("transactionDate", StringType, true) //FIXME switch to date
        ))

    val transSchemaRDD = transactions.transform(tranRDD=>{
      val transSchemaRDD: SchemaRDD = sqlContext.applySchema(tranRDD, schema)
      transSchemaRDD.registerTempTable("transactions")
      transSchemaRDD
    })
*/
    //import sqlContext.createSchemaRDD
    //registerTempTable("transactions")
    /**
     * requires messages to be in the format of queryId:query
     * this assumes the query is on the transactions table
     * for example: 1:SELECT count(*) as tran_count from transactions
     */
    val queries = queryStream

    /**
     * Combines both dstreams to a given single stream
     */
     /*queries.transformWith(transactions, (queryRDD, transRDD: RDD[Transaction])=>{
      queryRDD.map(queryMessage=>{
     /*   val schema = new StructType(Seq(
          StructField("taxId", StringType, true),
          StructField("name", StringType, true),
          StructField("merchant", StringType, true),
          StructField("amount", DoubleType, true),
          StructField("transactionDate", StringType, true) //FIXME switch to date
        ))
        val transSchemaRDD: SchemaRDD = sqlContext.applySchema(transRDD, schema)

        */
        sqlContext.sql(queryMessage)
      })
     }).print()
     */
    //import sqlContext.createSchemaRDD
    queries.transformWith(transactions, (queriesRDD: RDD[String], t: RDD[Transaction])=>{
      val queries = queriesRDD.collect()
      if(queries.length>0){
      val query = queries(0)
        import sqlContext.createSchemaRDD
        t.registerTempTable("transactions")
        t.map(x=>"registered")
      }else{
        t.map(x=>"not registered")
      }
    }).print()

    ssc
  }
}
