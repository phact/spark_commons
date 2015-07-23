package pro.foundev.benchmarks.spark_throughput

import com.datastax.bdp.spark.DseSparkContext
import org.apache.spark.{SparkConf, SparkContext}
import pro.foundev.benchmarks.spark_throughput.launchers._
import pro.foundev.commons.benchmarking.StdPrintService

object OperationThroughputBenchmarkRunner {
  /**
   * Launches the Spark job that runs the benchmarks. Suggested startup script is
   * dse spark-submit spark_throughput-assembly.jar $DRIVER_HOST master:///$MASTER_HOST:7077
   * @param args requires only one argument which is where the driver host is located
   */
  def main(args: Array[String])={

    val sc: SparkContext = DseSparkContext(new SparkConf()
      .set("driver.host", args(0))
      .setAppName("spark throughput")
      .set("spark.eventLog.enabled", "true")
      //necessary to set jar for api submission
      .setJars(Array("spark_throughput-assembly.jar"))
      .setMaster(args(1))
    )
    val printer = new StdPrintService()
    val tableSuffixes = Array("10k", "100k", "10m")
    val runBenches = ( fac: (SparkContext, String) => BenchmarkLauncher )=>{
      val benches = tableSuffixes.map(s=>fac(sc, s))
      new BenchmarkRun(benches, printer).exec()
    }
    runBenches((sc, s)=>new CogroupBenchmarkLauncher(sc,s))
    runBenches((sc, s)=>new CountBenchmarkLauncher(sc,s))
    runBenches((sc, s)=>new CountByBenchmarkLauncher(sc,s))
    runBenches((sc, s)=>new DistinctBenchmarkLauncher(sc,s))
    runBenches((sc, s)=>new FilterBenchmarkLauncher(sc,s))
    runBenches((sc, s)=>new FirstBenchmarkLauncher(sc,s))
    runBenches((sc, s)=>new GroupByBenchmarkLauncher(sc,s))
    runBenches((sc, s)=>new IntersectBenchmarkLauncher(sc,s))
    runBenches((sc, s)=>new JoinBenchmarkLauncher(sc,s))
    runBenches((sc, s)=>new MaxBenchmarkLauncher(sc,s))
    runBenches((sc, s)=>new MinBenchmarkLauncher(sc,s))
    runBenches((sc, s)=>new ReduceBenchmarkLauncher(sc,s))
    runBenches((sc, s)=>new TakeBenchmarkLauncher(sc,s))
    runBenches((sc, s)=>new UnionBenchmarkLauncher(sc,s))
  }
}
