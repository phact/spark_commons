import com.datastax.bdp.spark.DseSparkContext
import com.datastax.spark.connector._
import org.apache.spark.sql.cassandra.CassandraSQLContext
import org.apache.spark.{SparkConf, SparkContext}

def time[R](callback: () => R): R = {
  val t0 = System.nanoTime()
  val result = callback()
  val t1 = System.nanoTime()
  println((t1 - t0)/1000000000)
  result
}
val sc: SparkContext = DseSparkContext(new SparkConf()
  .set("driver.host", "127.0.0.1")
  .set("spark.cassandra.connection.host", "127.0.0.1")
  .setAppName("spark throughput")
  .set("spark.eventLog.enabled", "true")
  //necessary to set jar for api submission
  .setJars(Array("spark_throughput-assembly.jar"))
  .setMaster("spark://127.0.0.1:7077")
)

val csc = new CassandraSQLContext(sc)
val pairRDD = sc.cassandraTable("spark_test", "records_10m").map(x=>(x.getUUID(0), x.getLong(1)))

time(()=> sc.cassandraTable("spark_test", "records_10m").map(x=>x.getLong(1)).reduce((v1,v2)=>{if(v1>v2){v1};else{v2}}))
time(()=> csc.sql("SELECT MAX(c0) FROM spark_test.records_10m").collect()(0)(0))
