/*
 * Copyright 2015 Foundational Development
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package pro.foundev.commons.test_support

import akka.actor.ActorSystem
import akka.util.ByteString
import akka.zeromq.{Bind, SocketType, ZMQMessage, ZeroMQExtension}

class ZeroMQTestPublisher (url: String, topicName: String) extends Serializable{
  lazy val actorSystem = ActorSystem()
  lazy val socket = ZeroMQExtension(actorSystem).newSocket(SocketType.Pub, Bind(url))

  def publish(message: List[ByteString])={
    val frames =  ByteString(topicName) :: message
    socket ! ZMQMessage(frames.toSeq)
  }

  def cleanUp(): Unit ={
    actorSystem.stop(socket)
  }
}
