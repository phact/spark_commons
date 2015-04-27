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
package pro.foundev.messaging

import com.rabbitmq.client.ConnectionFactory

class RabbitMQPublisher(host: String, reportExchangeName: String) extends LogPublisher{
  private val factory = new ConnectionFactory()
  factory.setHost(host)
  private val connection = factory.newConnection()
  private val channel = connection.createChannel()

  override def publish(message: String): Unit = {
    channel.basicPublish(reportExchangeName, "", null,message.getBytes())
  }

  def stop() = {
    channel.close()
    connection.close()
  }

}
