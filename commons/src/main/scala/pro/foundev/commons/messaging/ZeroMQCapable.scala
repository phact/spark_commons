/*
 *
 *   Copyright 2015 Foundational Development
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



package pro.foundev.commons.messaging

import akka.util.ByteString
import akka.zeromq.Subscribe
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.zeromq._

/**
 * Easy DStream creation for ZeroMQ. Already handles conversion of ByteString back to String
 */
trait ZeroMQCapable {
  def bytesToStringIterator(x: Seq[ByteString]): Iterator[String] = {
    println(x)
    x.map(_.utf8String).iterator
  }

  /**
   * Convenience wrapper for ZeroMQUtils
   * @param ssc
   * @param url zeromq url such as tcp://127.0.0.1:1234
   * @param subscriberTopic should match what the publisher is sending minus anything after a period. example
   *              - pub: foo.bar sub: foo - valid
   *              - pub: foo sub: foo - valid
   * @return Assumes DStream of strings. TODO: generics
   */
  def createQueueDStream(ssc: StreamingContext, url: String, subscriberTopic: String ):DStream[String]={
    ZeroMQUtils.createStream(ssc, url, Subscribe(subscriberTopic), bytesToStringIterator _)
  }
}
