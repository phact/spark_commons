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
package pro.foundev.messaging

import akka.actor.ActorSystem
import akka.util.ByteString
import akka.zeromq.{Bind, SocketType, ZMQMessage, ZeroMQExtension}

class ZeroMQPublisher (master: String) extends LogPublisher{
  override def publish(message: String): Unit = {
    val acs: ActorSystem = ActorSystem()
    val url: String = "tcp://"+master+":10050"
    val pubSocket = ZeroMQExtension(acs).newSocket(SocketType.Pub, Bind(url))
    implicit def stringToByteString(x: String): ByteString = ByteString(x)
    val messages: List[ByteString] = List(message.toString)
    pubSocket ! ZMQMessage(ByteString(topic) :: messages)
    acs.stop(pubSocket)
  }
  def topic():String = {
    "report.sessions"
  }
}
