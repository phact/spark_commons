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

import akka.actor.ActorSystem
import akka.util.ByteString
import akka.zeromq.{Bind, SocketType, ZMQMessage, ZeroMQExtension}
import org.scalatest.FunSpec
import pro.foundev.commons.RddAsserts
import pro.foundev.commons.streaming.StreamingObjectMother

class ZeroMQPublishTester (url: String, topicName: String) extends Serializable{
  lazy val actorSystem = ActorSystem()
  lazy val socket = ZeroMQExtension(actorSystem).newSocket(SocketType.Pub, Bind(url))

  def publish(message: List[ByteString])={
    socket ! ZMQMessage(ByteString(topicName) :: message)
  }

  def cleanUp(): Unit ={
    actorSystem.stop(socket)
  }
}
class ZeroMQCapableSpec extends FunSpec {
  describe("ZeroMQCapable"){
    it("will read published lines") {
      implicit def stringToByteString(x: String): ByteString = ByteString(x)
      val topicName = "fake-topic"
      val url = "tcp://127.0.0.1:10049"
      val publisher = new ZeroMQPublishTester(url, topicName)
      val zeroMQCapable = new ZeroMQCapable() {
      }
      val ssc = new StreamingObjectMother().createStream()
      val lines = zeroMQCapable.createQueueDStream(ssc, url, topicName)
      ssc.start()
      publisher.publish(List("A", "B", "C"))
      publisher.publish(List("A", "B", "C"))
      publisher.publish(List("A", "B", "C"))
      publisher.publish(List("A", "B", "C"))
      try {
        Thread.sleep(1000)
        RddAsserts.assertCountIs(lines, 5)
        //assert(results.length == 5)
      }finally {
        ssc.stop()
        publisher.cleanUp()
      }

    }
  }
}
