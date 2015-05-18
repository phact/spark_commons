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
import pro.foundev.calculations.CheapLogCalculator
import pro.foundev.commons.test_support.SparkSpec
import pro.foundev.dto.IpLog

class CheapLogCalculatorSpec extends FunSpec with SparkSpec with Matchers with BeforeAndAfterEach{
  var logCalculator: CheapLogCalculator = _

  override def beforeEach(){
    logCalculator = new CheapLogCalculator()
  }
  describe("empty urls"){
    it("should reduce down to 0"){
      val log1 = new IpLog(1, "tx", InetAddress.getLocalHost, List[String]())
      val log2 = new IpLog(2, "tx", InetAddress.getLoopbackAddress, List[String]())
      val list = List[IpLog](log1, log2)
      val logRDD = sc.parallelize(list)
      val results = logCalculator.totalUrls(logRDD)
      results should equal(0)
    }
  }
  describe("4 total urls"){
    it("should reduce down to 4 total urls"){
      val log1 = new IpLog(1, "tx", InetAddress.getLocalHost, List[String]("1", "2", "3"))
      val log2 = new IpLog(2, "tx", InetAddress.getLoopbackAddress, List[String]("4"))
      val list = List[IpLog](log1, log2)
      val logRDD = sc.parallelize(list)
      val results = logCalculator.totalUrls(logRDD)
      results should equal(4)
    }

  }
  describe("emptyRDD"){
    it("should reduce down to 0"){
      val list = List[IpLog]()
      val logRDD = sc.parallelize(list)
      val results = logCalculator.totalUrls(logRDD)
      results should equal(0)
    }
  }
}
