package tech.mlsql.plugins.mllib.ets.fe

/**
 * 8/9/2022 WilliamZhu(allwefantasy@gmail.com)
 */
object BenchmarkFunction {
  def main(args: Array[String]): Unit = {
    val v = System.currentTimeMillis()
    val times = 10000000
    for (i <- 0 until 10000000) {
      SQLPatternDistribution.find_alternativePatterns("dfeadfaefaxxxxxfeafeafe", 100)
    }
    println("tt:" + (System.currentTimeMillis() - v) / 1000)
  }
}
