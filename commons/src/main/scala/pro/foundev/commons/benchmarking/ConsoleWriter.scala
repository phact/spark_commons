package pro.foundev.commons.benchmarking

class ConsoleWriter extends OutputWriter{
  override def println(message: String): Unit = {
    println(message)
  }
  override def print(message: String): Unit = {
    print(message)
  }
}
