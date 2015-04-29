package pro.foundev.commons.benchmarking

trait OutputWriter {
  def print(message: String): Unit
  def println(message: String):Unit
}
