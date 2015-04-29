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

package pro.foundev.examples.spark_streaming.sockets

import com.datastax.spark.connector.cql.CassandraConnector
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream
import pro.foundev.examples.spark_streaming.cassandra.CassandraCapable

trait TextSocketCapable extends CassandraCapable {

  val port = 10034
  def connectToSocket(hostName: String): (DStream[String], StreamingContext, CassandraConnector) = {
    val context = connect(hostName)
    val rdd: DStream[String] = context.streamingContext.socketTextStream(hostName,port)
    (rdd, context.streamingContext, context.connector)
  }
}
