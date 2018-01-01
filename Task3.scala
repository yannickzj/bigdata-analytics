import org.apache.spark.{SparkContext, SparkConf}

object Task3 {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Task 3")
    val sc = new SparkContext(conf)

    val textFile = sc.textFile(args(0))

    val output = textFile
      .flatMap(line => {
        val tokens = line.split(",", -1)
        tokens.slice(1, tokens.length)
          .zipWithIndex
          .map{case (rate, index) => {
            var count = 1
            if (rate.length() == 0) count = 0
            (index, count)
          }}})
      .reduceByKey((x, y) => x + y)
      .map{case (index, num) => index + "," + num}

    output.saveAsTextFile(args(1))
  }
}
