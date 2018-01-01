import org.apache.spark.{SparkContext, SparkConf}

object Task2 {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Task 2")
    val sc = new SparkContext(conf)

    val textFile = sc.textFile(args(0))

    val output = textFile.map(line => ("*", line.split(",").count(token => token.length() > 0) - 1))
      .reduceByKey((x, y) => x + y)
      .values

    output.repartition(1).saveAsTextFile(args(1))
  }
}
