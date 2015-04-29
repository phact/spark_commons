package pro.foundev.commons.benchmarking

import org.scalatest.FunSpec
import org.scalatest.mock.MockitoSugar
import org.mockito.Mockito._

class BenchmarkResultPrinterSpec extends FunSpec with MockitoSugar{
  describe("BenchmarkResultPrinter"){
    describe("single result"){
      val runner = mock[BenchmarkRunner]
      val outputWriter = mock[OutputWriter]
      val benchmarkPrinter = new BenchmarkResultPrinter(runner, outputWriter)
      it("prints out the individual result"){
        benchmarkPrinter.outputReports
        when(runner.exec).thenReturn(Map("streaming",
          Seq[BenchmarkReport](new BenchmarkReport(1.25, "this is my report"))))
        verify(outputWriter, times(1)).print("----------------------------\n"+
                                             "| streaming                |\n" +
                                             "----------------------------\n" +
                                             "| time | bench name        |\n" +
                                             "| 1.25 | this is my report |\n" +
                                             "---------------------------"
        )
      }
    }
    describe("multiple results one tab")
    describe("multiple results by tag")
  }
}
