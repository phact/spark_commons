package pro.foundev.scala


object FindWithArgs extends CassandraCapable {

  def main(args: Array[String]): Unit = {

    val context = connectToCassandra()
    val rdd = context.rdd
    var version = "1"
    if(args.length>0) {
      if (version != null) {
        version = args(0)
      }
    }
    println("reading version: " + version)
    rdd.where("version=" + version).
      map(x => x.get[String]("value")).collect()
      .foreach(x => println(x))
  }
}
