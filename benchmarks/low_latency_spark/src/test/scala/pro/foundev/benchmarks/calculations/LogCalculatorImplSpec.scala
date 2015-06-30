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

package pro.foundev.benchmarks.calculations

import java.net.InetAddress

import org.scalatest.{BeforeAndAfterEach, FunSpec, Matchers}
import pro.foundev.calculations.{LogCalculator, LogCalculatorImpl}
import pro.foundev.commons.test_support.SparkSupport
import pro.foundev.dto.{IpLog, SessionReport}

class LogCalculatorImplSpec extends FunSpec with SparkSupport with Matchers with BeforeAndAfterEach{
  var logCalculator: LogCalculator = _

  override def beforeEach(){
   logCalculator = new LogCalculatorImpl()
  }
   def buildSession(list: List[IpLog]): SessionReport = {
     val logRDD = sc.parallelize(list)
     logCalculator.sessionReport(logRDD)
   }

  describe("empty urls"){
    def buildList(): List[IpLog] ={
      val log1 = new IpLog(1, "TX", InetAddress.getLocalHost, List[String]("127.0.0.1", "122.12.12.12"))
      val log2 = new IpLog(2, "IN", InetAddress.getLoopbackAddress, List[String]())
       List[IpLog](log1, log2)
    }

    it("should return just first state"){
      val sessionReport = buildSession(buildList())
      sessionReport.biggestState should equal("TX")
    }
    it("should return an average of 0"){
      val sessionReport = buildSession(buildList())
      sessionReport.averageClicksPerSession should equal(1.0)
    }
  }

  describe("different states with populated urls"){
    def buildLogs(): List[IpLog] = {
      val txLog1 = new IpLog(1, "MN", InetAddress.getLocalHost, List[String]("10", "11", "12", "13"))
      val inLog1 = new IpLog(2, "IN", InetAddress.getLoopbackAddress, List[String]("1"))
      val inLog2 = new IpLog(3, "IN", InetAddress.getLoopbackAddress, List[String]("2", "4"))
      val inLog3 = new IpLog(4, "IN", InetAddress.getLoopbackAddress, List[String]("3", "5"))
      List[IpLog](txLog1, inLog1, inLog2, inLog3)
    }

    it("should give us state with largest total URL count"){
      val session = buildSession(buildLogs())
      session.biggestState should equal("IN")
    }
    it("should give us averages of all users click count"){
      val session = buildSession(buildLogs())
      // 4 + 1 + 2 + 2 = 9/4
      session.averageClicksPerSession should equal(2.25)
    }

  }
  describe("emptyRDD"){
    it("should raise exception"){
      val list = List[IpLog]()
      val logRDD = sc.parallelize(list)
      an [UnsupportedOperationException] should be thrownBy logCalculator.sessionReport(logRDD)
    }
  }

}
