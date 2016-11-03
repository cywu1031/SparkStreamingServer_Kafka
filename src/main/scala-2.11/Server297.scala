import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._

object Server297 {
    def main(args: Array[String]) {
        val conf = new SparkConf().setMaster("local[*]").setAppName("Server297")
        val ssc = new StreamingContext(conf, Seconds(5))
        val kafkaStream = KafkaUtils.createStream(ssc, "localhost:2084","spark-streaming-consumer-group", Map("yelp" -> 5))
        val lines = kafkaStream.map(_._2)
        val words = lines.flatMap(_.split(" "))

        words.foreachRDD ( rdd => {
            for (item <- rdd.collect().toArray) {
                println(item.toString)
            }
        })

        sys.ShutdownHookThread {
            ssc.stop(true, true)
            println("Application stopped")
        }

        ssc.start
        ssc.awaitTermination
    }
}
