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

package pro.foundev.examples.spark_streaming.messaging

import com.datastax.spark.connector.cql.CassandraConnector
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream
import pro.foundev.examples.spark_streaming.cassandra.CassandraCapable
import pro.foundev.examples.spark_streaming.java.messaging.RabbitMQReceiver

abstract class RabbitMqCapable(master: String, checkPointDirectory: String) extends CassandraCapable{

  def connectToExchange(): (DStream[String], StreamingContext, CassandraConnector) = {
    val context = connect(master)
    val ssc = context.streamingContext
    ssc.checkpoint(checkPointDirectory)
    val customReceiverStream = ssc
      .receiverStream(new RabbitMQReceiver(StorageLevel.MEMORY_AND_DISK_2, master))
    val dstream: DStream[String] = customReceiverStream
    (dstream, context.streamingContext, context.connector)
  }

  def createContext():StreamingContext

  def startJob() = {
    val context = StreamingContext.getOrCreate(
      "cfs:///" + checkPointDirectory, ()=>createContext())
    context.start()
    context.awaitTermination()
  }
}
