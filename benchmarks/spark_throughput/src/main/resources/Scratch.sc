import java.io.{FileWriter, BufferedWriter}

def profile[R](callback: () => R): R = {
  val t0 = System.nanoTime()
  val result = callback()
  val t1 = System.nanoTime()
  println((t1 - t0)/1000000.0)
  result
}

import scala.math._

  implicit class ExtendedDouble(n: Double) {
    def rounded(x: Int) = {
      val w = pow(10, x)
      (n * w).toLong.toDouble / w
    }
  }


def clean_file(file_name:String)= {
  val file = scala.io.Source.fromFile(file_name)
  val cleaned = file.getLines()
    .filter(x => x != "benchmark done")
  val newlyCleaned = cleaned
    .filter(x => x != "start benchmarks")
  val noBench = newlyCleaned
  .map(x => x.replace(" milliseconds to run ", ","))
  .map(x => x.replace(" on ", ","))
  val bw = new BufferedWriter(new FileWriter(file_name+".csv"))
  noBench.foreach(x=>bw.write(x+"\n"))
  bw.flush()
  bw.close()
}
val baseFileDir = "/"
(2 until 11).foreach(l=>(clean_file(baseFileDir + "test_run_" + l + ".txt")))

val bw = new BufferedWriter(new FileWriter(baseFileDir + "final-report.txt"))
bw.write("ops,count,,min,max,avg\n")
val map = (1 until 11).flatMap(i=> {
  val file = scala.io.Source.fromFile(baseFileDir + "test_run_" + i + ".txt.csv")
  file.getLines()
    .map(x => x.split(","))
    .map(x=>(x(1),(x(2), (i, x(0)))))
    //.map(x => (x(1) + ":" + x(2), (i, x(0))))
}).groupBy(x=>x._1)
.foreach(x=>{
  val operation = x._1
  val groupedRun = x._2
    .groupBy(x=>x._2._1)
    .map(x=>{
    val recordCount = x._1
    val numbers = x._2.map(x=>x._2._2._2.toDouble.rounded(2))
    val min = numbers.min
    val max = numbers.max
    val avg = numbers.sum/10.0
    operation+","+recordCount+","+min+","+max+"," + avg.rounded(2)
  })
    groupedRun
  .foreach(x=> {
    bw.write("\t"+x+"\n")
  })
 // bw.flush()
 // bw.close()
})
bw.flush()
bw.close()


