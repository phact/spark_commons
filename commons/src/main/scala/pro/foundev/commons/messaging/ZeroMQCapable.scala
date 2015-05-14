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

import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.zeromq._
import akka.zeromq._
import akka.zeromq.Subscribe
import akka.util.ByteString

trait ZeroMQCapable {
  def bytesToStringIterator(x: Seq[ByteString]): Iterator[String] = x.map(_.utf8String).iterator

  def createQueueDStream(ssc: StreamingContext, url: String, topic: String ):DStream[String]={
    ZeroMQUtils.createStream(ssc, url, Subscribe(topic), bytesToStringIterator _)
  }
}
