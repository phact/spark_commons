package pro.foundev.commons.benchmarking

class BenchmarkResultPrinter(benchmarkRunner: BenchmarkRunner, outputWriter: OutputWriter) {
  def outputReports: Unit = {
    val reports = benchmarkRunner.exec
    val maxLengthForTitles:Int = reports.map(x=>x._1.length).max
    val maxLengthForReports: Int = reports.flatMap(x=>x._2).map(y=>y.name.length+3+y.timeElapsed.toString.length).max
    val strLength: Int = maxLengthForReports.max(maxLengthForTitles) + 3
    reports.foreach((m)=>{
      val separator:String = "-".padTo(strLength, "-")
      outputWriter.print()
      (m._1
      m._2.foreach()
    })
  }

}
