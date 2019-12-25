import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author ming
  * @date 2019/12/16 16:58
  */
object CountNoPath {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("ming-vehicle-count-no-path")
    sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    sparkConf.setMaster("local[*]")
    val sc = new SparkContext(sparkConf)

    sc.textFile("file:///D://data")
      .map(x => {
        val params = x.split(",")
        val stringBuilder = new StringBuilder()
        val pathStartIndex = 6
        if (params.length > 6) {
          for (i <- pathStartIndex until  params.length) {
            stringBuilder.append(params(i))
            if (i < params.length - 1) {
              stringBuilder.append(",")
            }
          }
        }

        (params(0), params(1).toDouble, params(2).toDouble, params(3), params(4).toDouble, params(5).toDouble, stringBuilder.toString())
      })
      .filter(x => x._7 == "")
      .saveAsTextFile("file:///D://data-ret")

    sc.stop()
  }
}