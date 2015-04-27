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
package pro.foundev.reporting

import org.scalatest.{Matchers, FunSpec}

class BenchmarkReportSpec extends FunSpec with Matchers{
  describe("BenchmarkReport") {
    describe("empty") {
      it("has a nice message") {
        val benchmarkReport = new BenchmarkReport("My Strategy")
        val finalReport: String = benchmarkReport.generateFinalReport
        assert("There are no report items to display for 'My Strategy'" === finalReport)
      }
    }
    describe("when there is one item in the report") {
      val benchmarkReport = new BenchmarkReport("My Strategy")
        benchmarkReport.addLine(title = "new benchmark",
          min = 1.0,
          max = 10.1,
          percentile50th = 4.1,
          percentile90th = 9.1,
          average = 4.3)
      it("generates a friendly report string") {
        val report = benchmarkReport.generateFinalReport
        assert(report===
          "Benchmarks for strategy 'My Strategy'"+ System.lineSeparator()+
          "Report from new benchmark" + System.lineSeparator()+
          "------------------------" + System.lineSeparator()+
          "Average:         "+ 4.3 + System.lineSeparator()+
                        "Min:             "+ 1.0 + System.lineSeparator() +
                        "50th Percentile: "+ 4.1 + System.lineSeparator() +
          "90th Percentile: "+ 9.1 + System.lineSeparator() +
          "Max:             "+ 10.1 + System.lineSeparator()+
        "------------------------" + System.lineSeparator())
      }
    }
  }
}

