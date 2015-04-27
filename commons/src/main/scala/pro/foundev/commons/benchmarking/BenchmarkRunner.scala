package pro.foundev.commons.benchmarking

import org.apache.commons.lang.time.StopWatch

class BenchmarkRunner (benchmarks: Benchmark*){
  def exec: Map[String, Seq[BenchmarkReport]] = {
    benchmarks.map(b => {
      val watch = new StopWatch()
      watch.start()
      b.callback.apply()
      watch.stop()
      val milliseconds = watch.getTime
      val seconds: Double = milliseconds / 1000.0
      new Tuple2[String, BenchmarkReport](b.tag, new BenchmarkReport(seconds, b.name))
    }).groupBy(_._1)
      .map { case (k, v) => (k, v.map(_._2)) }
  }

}
