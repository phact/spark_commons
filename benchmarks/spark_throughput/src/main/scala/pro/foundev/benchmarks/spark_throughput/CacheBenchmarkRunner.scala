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
   * @param args requires four arguments: <driver host> <spark master> <nonkryoresultfile> <kyroresultfile>
   */
  def main(args: Array[String])={

   val driverHost = args(0)
   val master = args(1)
   val nonkryoFile = args(2)
   val kyroFile = args(3)
   val tableSuffixes = Array("10m")
   val levelsUnderTest = Array(StorageLevel.DISK_ONLY,
     StorageLevel.DISK_ONLY_2, StorageLevel.MEMORY_AND_DISK,
   StorageLevel.MEMORY_AND_DISK_2, StorageLevel.MEMORY_AND_DISK_SER,
     StorageLevel.MEMORY_AND_DISK_SER_2, StorageLevel.MEMORY_ONLY,
   StorageLevel.MEMORY_ONLY_2, StorageLevel.MEMORY_ONLY_SER, StorageLevel.MEMORY_ONLY_SER_2)

   val runBenches = (sc:SparkContext, printer: FilePrintService)=>( fac: (SparkContext, String) => BenchmarkLauncher )=>{
     val benches = tableSuffixes.map(s=>fac(sc, s))
     new BenchmarkRun(benches, printer).exec()
   }

   val nonKyroFilePrinter = new FilePrintService(nonkryoFile)
   val noKyro = createSC(false, driverHost, master)
   levelsUnderTest.foreach((st)=>
   runBenches(noKyro,nonKyroFilePrinter)((sc, s)=>new CacheBenchmarkLauncher(sc,s, st)))
   noKyro.stop()
   nonKyroFilePrinter.close()

   val kryoFilePrinter = new FilePrintService(kyroFile)
   val withKyro = createSC(true, driverHost, master)
   levelsUnderTest.foreach((st)=>
     runBenches(withKyro, kryoFilePrinter)((sc, s)=>new CacheBenchmarkLauncher(sc,s, st)))
   withKyro.stop()
   kryoFilePrinter.close()
  }

  def createSC(withKyro: Boolean, driverHost: String, master: String): SparkContext = {
    var sparkConf = new SparkConf()
    var appName = "spark cache throughput"
    if(withKyro){
      sparkConf = sparkConf.set("spark.serializer","org.apache.spark.serializer.KryoSerializer")
      appName = "spark cache throughput with Kyro"
    }
    DseSparkContext(sparkConf
      .set("driver.host", driverHost)
      .setAppName(appName)
      .set("spark.eventLog.enabled", "true")
      //necessary to set jar for api submission
      .setJars(Array("spark_throughput-assembly.jar"))
      .setMaster(master)
    )
  }
}
