package pro.foundev.commons.benchmarking

import org.scalatest.FunSpec

class BenchmarkRunnerSpec extends FunSpec {
  describe("BenchmarkRunner when running benchmarks"){
    var benchmarkRunner: BenchmarkRunner = null
    describe("when empty"){
      benchmarkRunner = new BenchmarkRunner()
      it("creates raises an exception"){

      }
    }
    describe("when more than one") {
      var benchmarkOneCalled = false
      val benchmarkOneCallBack = ()=> {
        benchmarkOneCalled = true
        Thread.sleep(10)
      }
      val benchmarkOneName = "benchmark_one"
      val benchmarkOneTag = "benchmark_one_tag"
      val benchmarkOne = new Benchmark(benchmarkOneCallBack, benchmarkOneName, benchmarkOneTag )
      var benchmarkTwoCalled = false
      val benchmarkTwoCallBack = ()=> {
        benchmarkTwoCalled = true
        Thread.sleep(10)
      }
      val benchmarkTwoName = "benchmark_two"
      val benchmarkTwoTag = "benchmark_two_tag"
      val benchmarkTwo = new Benchmark(benchmarkTwoCallBack, benchmarkTwoName, benchmarkTwoTag)
      benchmarkRunner = new BenchmarkRunner(benchmarkOne, benchmarkTwo)
      it("runs each benchmark"){
        benchmarkRunner.exec
        assert(benchmarkOneCalled)
        assert(benchmarkTwoCalled)
      }
      it("generates a report for each benchmark"){
        val benchmarkReports = benchmarkRunner.exec
        val firstReportList = benchmarkReports(benchmarkOneTag)
        assert(firstReportList.length == 1)
        val firstReport = firstReportList(0)
        assert(firstReport.timeElapsed > 0.01)
        assert(firstReport.name ==  benchmarkOneName)
        val secondReportList = benchmarkReports(benchmarkTwoTag)
        assert(secondReportList.length==1)
        val secondReport = secondReportList(0)
        assert(secondReport.timeElapsed > 0.01)
        assert(secondReport.name ==  benchmarkTwoName)
      }
    }
  }
}
