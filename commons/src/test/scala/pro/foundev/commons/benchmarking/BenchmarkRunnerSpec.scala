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

package pro.foundev.commons.benchmarking

import org.scalatest.FunSpec

class BenchmarkRunnerSpec extends FunSpec {
  describe("BenchmarkRunner when running benchmarks") {
    describe("when empty") {
      it("creates raises an exception") {
        val benchmarkRunner = new BenchmarkRunner()
        intercept[IllegalArgumentException] {
          benchmarkRunner.exec
        }
      }
    }
    describe("when more than one") {
      var benchmarkTwoCalled = false
      var benchmarkOneCalled = false
      val benchmarkOneName = "benchmark_one"
      val benchmarkOneTag = "benchmark_one_tag"
      val benchmarkTwoName = "benchmark_two"
      val benchmarkTwoTag = "benchmark_two_tag"
      benchmarkTwoCalled = false
      benchmarkOneCalled = false
      val benchmarkOneCallBack = () => {
        benchmarkOneCalled = true
        Thread.sleep(20)
      }
      val benchmarkOne = new Benchmark(benchmarkOneCallBack, benchmarkOneName, benchmarkOneTag)
      val benchmarkTwoCallBack = () => {
        benchmarkTwoCalled = true
        Thread.sleep(20)
      }
      val benchmarkTwo = new Benchmark(benchmarkTwoCallBack, benchmarkTwoName, benchmarkTwoTag)
      val benchmarkRunner = new BenchmarkRunner(benchmarkOne, benchmarkTwo)
      it("runs each benchmark") {
        benchmarkRunner.exec
        assert(benchmarkOneCalled)
        assert(benchmarkTwoCalled)
      }
      it("generates a report for each benchmark") {
        val benchmarkReports = benchmarkRunner.exec
        val firstReportList = benchmarkReports(benchmarkOneTag)
        assert(firstReportList.length == 1)
        val firstReport = firstReportList(0)
        assert(firstReport.timeElapsed > 0.01)
        assert(firstReport.name == benchmarkOneName)
        val secondReportList = benchmarkReports(benchmarkTwoTag)
        assert(secondReportList.length == 1)
        val secondReport = secondReportList(0)
        assert(secondReport.timeElapsed > 0.01)
        assert(secondReport.name == benchmarkTwoName)
      }
    }
  }
}
