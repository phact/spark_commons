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

import org.apache.spark.sql.Row
import org.apache.spark.sql.cassandra.CassandraSQLContext
import org.apache.spark.streaming.StreamingContext
import pro.foundev.examples.spark_streaming.messaging.RabbitMqCapable
import pro.foundev.examples.spark_streaming.utils.Args

/**
 * How to Run
 *
 *  - checkout source
 *  - install sbt
 *  - sbt assembly
 *  - make sure rabbitmq-server is 3.4.4 and bound on all interfaces that you need to connect to and make sure it's running
 *  - dse spark-submit --class pro.foundev.examples.spark_streaming.SqlQueryEngine dse_spark_streaming_examples-assembly-0.4.0.jar |ip_address|
 *  - rabbitmqadmin publish exchange=queries payload="1:SELECT * FROM tester.streaming_demo" routing_key=null
 */
object SqlQueryEngine {
  def main(args: Array[String]) = {
    val master = Args.parseMaster(args)
    new SqlQueryEngine(master).startJob()
  }
}

class SqlQueryEngine(master: String) extends RabbitMqCapable(master, "sql_query_engine"){
  override def createContext(): StreamingContext = {
    val (dstream, ssc, connector) = connectToExchangeNamed("queries")
    val csc = new CassandraSQLContext(ssc.sparkContext)
    val emptyRDD = csc.sparkContext.emptyRDD[Row]

    /**
     * 1. Splits message by the : separator.
     * 2. Makes the query id and query part of a tuple
     * 3. maps the queries and their id's to results
     * 4. Finally prints the result
     */
    dstream
      .map(x => x.split(":"))
      .map(x => (x(0), x(1)))
      .transform(rdd => {
      /**
       * naive example to print RDD, would in production write out
       * response to external system here using foreach
       */
      val queries = rdd.collect()
      if (queries.length > 0) {
        val queryMessage = queries(0)
        val query = queryMessage._2

        /**
         * Not sure why we have to cache but we do or we get an exception
         */
        csc.sql(query).cache()
      } else {
        emptyRDD
      }
    }).print()

    ssc
  }
}
