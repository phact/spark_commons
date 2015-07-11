def profile[R](callback: () => R): R = {
  val t0 = System.nanoTime()
  val result = callback()
  val t1 = System.nanoTime()
  println((t1 - t0)/1000000.0)
  result
}
