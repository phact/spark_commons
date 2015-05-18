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
import org.scalatest.concurrent.Eventually
import org.scalatest.{FlatSpec, GivenWhenThen, Matchers}
import pro.foundev.commons.test_support.{ZeroMQTestPublisher, RDDCounter, SparkStreamingSpec}

import scala.collection.mutable

/**
 * have to use serializable class or will get hit by all sorts of pain
 * - AssertionHelper decorates if you use an anonymous class
 * -
 * needs zeromq 2.2. On mac: brew install homebrew/versions/zeromq22
 */
class ZeroMQCapableImpl extends ZeroMQCapable with Serializable{

}
class ZeroMQCapableSpec extends FlatSpec with SparkStreamingSpec with GivenWhenThen with Matchers with Eventually{
  // default timeout for eventually trait
//  implicit override val patienceConfig =
 //   PatienceConfig(timeout = scaled(Span(1500, Millis)))

  "ZeroMQCapable" should "read lines" in {
    System.clearProperty("spark.master.port")
    Given("streaming context is initialized")
    val topicName = "fake-topic"
    val url = "tcp://127.0.0.1:10049"
    val publisher = new ZeroMQTestPublisher(url, topicName )
    val zeroMQCapable = new ZeroMQCapableImpl()
    var results= mutable.ArrayBuffer.empty[Array[String]]
    val lines = zeroMQCapable.createQueueDStream(ssc, url, topicName)
    //Have to use RDDCounter otherwise serialization on the closure kicks in .
    // TODO verify further and understand why it is guarded inside the { }
    RDDCounter.count(lines){
      (rdd)=>results += rdd.collect()
    }
    publisher.publish(List(ByteString("FIXME need this to make anything else work..no idea why")))
    ssc.start()
    Thread.sleep(300) //FIXME this is probably related to issues with the above bogus publish
    publisher.publish(List(ByteString("A")))
    publisher.publish(List(ByteString("D")))
    publisher.publish(List(ByteString("G")))
    publisher.publish(List(ByteString("H")))
    publisher.publish(List(ByteString("K")))
    Thread.sleep(300) //FIXME figure out why I need this long for item to register
    Thread.sleep(100)


    When("publishing 5 messages")
    try {
      When("after first slide")
      clock.advance(batchDuration.milliseconds)
      Then("5 should be read back")
      eventually {
        results.last.length should equal(5)
      }

    }finally{
      publisher.cleanUp()
      //System.clearProperty("spark.master.port")
    }
  }
}
