package pro.foundev.commons.benchmarking

case class Benchmark(callback: () => Unit, name: String, tag: String)

