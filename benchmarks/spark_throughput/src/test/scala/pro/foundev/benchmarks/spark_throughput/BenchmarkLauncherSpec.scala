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

package pro.foundev.benchmarks.spark_throughput

import org.scalatest._
import pro.foundev.commons.test_support._

class BenchmarkLauncherSpec extends CommonsTestSupport {

  var benchmarkLauncher: BenchmarkLauncher = _
  var timer: MockTimer = _
  var maxResult: Result = _

  before {
    timer = new MockTimer()
    benchmarkLauncher = new BenchmarkLauncher(sc, timer)
    makeKeyspace("keyspace1")
    makeTable("keyspace1.standard1", Seq(("key","int"), ("value","int")), "key")
    cql("INSERT INTO keyspace1.standard1 (key, value) values (1, 5)")
    cql("INSERT INTO keyspace1.standard1 (key, value) values (2, 10)")
    cql("INSERT INTO keyspace1.standard1 (key, value) values (3, 1)")
  }

  "A BenchmarkLauncher" should "get a max value" in {
    benchmarkLauncher.max.value should be (10)
  }
  it should "have the name of max" in {
    benchmarkLauncher.max.name should be ("max")
  }
  it should "time the result" in {
    timer.setDuration(2000)
    benchmarkLauncher.max.milliSeconds should be (0.002)
  }
  "A BenchmarkLauncher" should "get a sqlMax value" in {
    benchmarkLauncher.sqlMax.value should be (10)
  }
  it should "have the name of sqlMax" in {
    benchmarkLauncher.sqlMax.name should be ("sqlMax")
  }
  it should "time the result of sqlMax" in {
    timer.setDuration(2000)
    benchmarkLauncher.sqlMax.milliSeconds should be (0.002)
  }
  "A BenchmarkLauncher" should "get an abbreviatedMax value" in {
    //this matches the first value inserted
    benchmarkLauncher.abbreviatedMax.value should be (1)
  }
  it should "have the name of abbreviatedMax" in {
    benchmarkLauncher.abbreviatedMax.name should be ("abbreviatedMax")
  }
  it should "time the result of abbreviatedMax" in {
    timer.setDuration(2000)
    benchmarkLauncher.abbreviatedMax.milliSeconds should be (0.002)
  }


}
