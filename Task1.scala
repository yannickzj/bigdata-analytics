import org.apache.spark.{SparkConf, SparkContext}

object Task1 {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Task 1")
    val sc = new SparkContext(conf)

    val textFile = sc.textFile(args(0))

    val output = textFile.map(line => {
      val tokens = line.split(",")
      val sb = new StringBuilder()

      var max = 0
      for (i <- 1 until tokens.length) {
        if (tokens(i).length() > 0) {
          val v = Integer.parseInt(tokens(i))
          if (v > max) {
            sb.setLength(0)
            sb.append(i)
            max = v
          } else if (v == max) {
            sb.append("," + i)
          }
        }
      }
      tokens(0) + "," + sb
    })
    
    output.saveAsTextFile(args(1))
  }

}
