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
import org.apache.spark.sql.cassandra.CassandraSQLContext
import org.apache.spark.streaming.{Seconds, StreamingContext}
import pro.foundev.examples.spark_streaming.messaging.RabbitMqCapable
import pro.foundev.examples.spark_streaming.utils.Args


object SqlOnWindows{
  def main(args:Array[String])={
    val master = Args.parseMaster(args)
    new SqlOnWindows(master).startJob()
  }
}
class SqlOnWindows(master: String) extends RabbitMqCapable(master, "sql_on_windows_checkpoint") {
  override def createContext(): StreamingContext = {
    val (dstream, ssc, connector) = connectToExchange()
    val sqlContext = new SQLContext(ssc.sparkContext)
    val format = new SimpleDateFormat("yyyy-MM-dd")

    import sqlContext.createSchemaRDD

    val findWarnings = (e: RDD[(String, (String, String, BigDecimal, Date))])=>{
      e.registerTempTable("transactions")
      sqlContext.sql("SELECT taxId, sum(amount) as sum_amount " +
        "FROM transactions " +
        "where sum_amount > 9999.00 " +
        "group by taxId")
    }
    //every 10 seconds look at the past 60 seconds
    dstream.window(Seconds(60), Seconds(10)).map(line=> {
      val columns = line.split(",")
      val taxId = columns(0)
      val name = columns(1)
      val merchant = columns(2)
      val amount = BigDecimal(columns(3))
      val transactionDate = format.parse(columns(4))
      println(line)
      (taxId, (name, merchant, amount, transactionDate))
    })
      .transform(x=>findWarnings(x))
    .foreachRDD(warningsRdd=>{
      warningsRdd.foreachPartition(warningIter=>{
        while(warningIter.hasNext) {
          println(warningIter.next())
        }
      })
    })
    ssc
  }
}
