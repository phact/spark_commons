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
import org.scalatest.mock.MockitoSugar

class BenchmarkResultPrinterSpec extends FunSpec with MockitoSugar{
  describe("BenchmarkResultPrinter"){
    describe("single result"){
      val runner = mock[BenchmarkRunner]
      val outputWriter = mock[OutputWriter]
      val benchmarkPrinter = new BenchmarkResultPrinter(runner, outputWriter)
      it("prints out the individual result"){
       /* benchmarkPrinter.outputReports
        when(runner.exec).thenReturn(Map("streaming",
          Seq[BenchmarkReport](new BenchmarkReport(1.25, "this is my report"))))
        verify(outputWriter, times(1)).print("----------------------------\n"+
                                             "| streaming                |\n" +
                                             "----------------------------\n" +
                                             "| time | bench name        |\n" +
                                             "| 1.25 | this is my report |\n" +
                                             "---------------------------"
        )
        */
        pending
      }
    }
    describe("multiple results one tab")(pending)
    describe("multiple results by tag")(pending)
  }
}
