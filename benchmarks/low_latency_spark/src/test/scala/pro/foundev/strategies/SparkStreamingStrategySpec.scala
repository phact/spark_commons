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
package pro.foundev.strategies

import com.datastax.driver.core.Cluster
import org.apache.spark.storage.StorageLevel
import org.scalatest.FunSpec
import org.scalatest.mock.MockitoSugar
import pro.foundev.Configuration
import pro.foundev.calculations.LogCalculatorImpl
import pro.foundev.ingest.SchemaCreation
import pro.foundev.messaging.{AbstractQueueReceiver, LogPublisher}
import pro.foundev.random.BenchmarkSeeding

import scala.collection.mutable

class MockPublisher extends LogPublisher{
  var list: mutable.MutableList[String] = new mutable.MutableList[String]()

  override def publish(message: String): Unit = {
    list.+=(message)
  }
}

class MockReceiver() extends AbstractQueueReceiver(StorageLevel.MEMORY_ONLY) {

  override def onStart() = {
  }
  override def onStop() = {
  }

  override def setHost(host: String): Unit = {}

  override def setQueueName(queueName: String): Unit = {}
}
class SparkStreamingStrategySpec extends FunSpec with MockitoSugar {
  describe("SparkStreamingStrategy"){
    describe("when data in database") {
      //hack this to get a basic test idea up and running
      val cluster = Cluster.builder().addContactPoint("127.0.0.1").build()
      val session = cluster.newSession()
      new SchemaCreation(session).createSchema()
      val result = session.execute("SELECT * FROM "+ Configuration.getKeyspace+"."+Configuration.getTable + " where id = 1")
      var state: String =  null
      val rows = result.all()
      if(rows.size()==0){
        state = "TX"
       session.execute("INSERT INTO " + Configuration.getKeyspace + "." + Configuration.getTable +
         " (id, origin_state, ip_address, urls) values ( 1, 'TX', '12.1.23.1', ['http://myhost.com/hostfinder'] )")
      }else{
        state = rows.get(0).getString("origin_state")
      }

      cluster.close()
      describe("on init") {
        val host = "local[2]"
        val logCalculator = new LogCalculatorImpl()
        /*val publisher = mock[ZeroMQPublisher]*/
        val publisher = new MockPublisher
        val receiver = new MockReceiver
        val sparkStreamingStrategy = new SparkStreamingStrategy(host, logCalculator, new BenchmarkSeeding(1000),
          receiver)
        it("stores master") {
          assert(sparkStreamingStrategy.getMaster === host)
        }
        /*

        it("it should be already be listening for messages with id") {
          val (url, topic) = ("tcp://127.0.0.1:10050", "benchmark.feed")
          val acs: ActorSystem = ActorSystem()

          val pubSocket = ZeroMQExtension(acs).newSocket(SocketType.Pub, Bind(url))
          implicit def stringToByteString(x: String): ByteString = ByteString(x)
          val messages: List[ByteString] = List("1")
          pubSocket ! ZMQMessage(ByteString(topic) :: messages)
          Thread.sleep(1000)
          assert(publisher.list.size == 1)
        }
        it("it should be already be listening for messages looking for origin state") {
          val (url, topic) = ("tcp://127.0.0.1:10050", "benchmark.feed")
          val acs: ActorSystem = ActorSystem()

          val pubSocket = ZeroMQExtension(acs).newSocket(SocketType.Pub, Bind(url))
          implicit def stringToByteString(x: String): ByteString = ByteString(x)
          val messages: List[ByteString] = List(state)
          pubSocket ! ZMQMessage(ByteString(topic) :: messages)
          Thread.sleep(1000)
          assert(publisher.list.size == 1)
        }
        it("it should be already be listening for messages for both")  {
          val (url, topic) = ("tcp://127.0.0.1:10050", "benchmark.feed")
          val acs: ActorSystem = ActorSystem()

          val pubSocket = ZeroMQExtension(acs).newSocket(SocketType.Pub, Bind(url))
          implicit def stringToByteString(x: String): ByteString = ByteString(x)
          val messages: List[ByteString] = List(state, "1")
          pubSocket ! ZMQMessage(ByteString(topic) :: messages)
          Thread.sleep(1000)
          assert(publisher.list.size == 2)
        }
        */
      }
    }

  }
}
