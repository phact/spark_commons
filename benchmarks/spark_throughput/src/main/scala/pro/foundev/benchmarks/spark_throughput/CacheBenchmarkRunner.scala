package pro.foundev.benchmarks.spark_throughput

import com.datastax.bdp.spark.DseSparkContext
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}
import pro.foundev.benchmarks.spark_throughput.launchers.{CacheBenchmarkLauncher, BenchmarkLauncher}
import pro.foundev.commons.benchmarking.FilePrintService

object CacheBenchmarkRunner  {
 /**
   * Launches the Spark job that runs the benchmarks. Suggested startup script is
   * dse spark-submit spark_throughput-assembly.jar $DRIVER_HOST master:///$MASTER_HOST:7077
   * @param args requires three arguments: <driver host> <spark master> <fileoutput>
   */
  def main(args: Array[String])={

    val sc: SparkContext = DseSparkContext(new SparkConf()
      .set("driver.host", args(0))
      .setAppName("spark cache throughput")
      .set("spark.eventLog.enabled", "true")
      //necessary to set jar for api submission
      .setJars(Array("spark_throughput-assembly.jar"))
      .setMaster(args(1))
    )
    val printer = new FilePrintService(args(2))
    val tableSuffixes = Array("10m")
    val runBenches = ( fac: (SparkContext, String) => BenchmarkLauncher )=>{
      val benches = tableSuffixes.map(s=>fac(sc, s))
      new BenchmarkRun(benches, printer).exec()
    }
   val levelsUnderTest = Array(StorageLevel.DISK_ONLY,
     StorageLevel.DISK_ONLY_2, StorageLevel.MEMORY_AND_DISK,
   StorageLevel.MEMORY_AND_DISK_2, StorageLevel.MEMORY_AND_DISK_SER,
     StorageLevel.MEMORY_AND_DISK_SER_2, StorageLevel.MEMORY_ONLY,
   StorageLevel.MEMORY_ONLY_2, StorageLevel.MEMORY_ONLY_SER, StorageLevel.MEMORY_ONLY_SER_2)
   levelsUnderTest.foreach((st)=>
   runBenches((sc, s)=>new CacheBenchmarkLauncher(sc,s, st)))
  }
}
