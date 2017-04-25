import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._

//import org.json.simple.JSONObject;
//import net.liftweb.json._
//import net.liftweb.json.JsonParser._
//import net.liftweb.json.Formats
//import _root_.kafka.serializer.StringDecoder

import org.apache.hadoop.hbase.{HBaseConfiguration,TableName}
import org.apache.hadoop.hbase.client.{ConnectionFactory,HBaseAdmin,HTable,Put,Get,Scan,Result,ResultScanner}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.io.ImmutableBytesWritable

//import org.apache.hadoop.hbase.mapreduce.{TableInputFormat}
//import org.apache.hadoop.hbase.mapred.{TableOutputFormat}
//import org.apache.hadoop.fs.Path
//import org.apache.hadoop.mapred.{JobConf}
//import scala.util.matching.Regex

object Server297 {
//    implicit val formats = DefaultFormats
    case class city(city1: String, city2: String)

    def saveRestaurantInfo(count: Long, item: String) = {
//        val key = Bytes.toString(item.getRow)
//        val title = parseTitle(Bytes.toString(item.getValue(Bytes.toBytes("data"), Bytes.toBytes("content"))))
//        println("rowkey["+key+"] title["+title+"]")
        val p = new Put(Bytes.toBytes(count))
        p.addColumn(Bytes.toBytes("info"), Bytes.toBytes("yelp"), Bytes.toBytes(item.toString))
        (new ImmutableBytesWritable, p)
    }

    def printRowKey(rs: ResultScanner) {
        val result = rs.next()
        if (result != null) {
            println(Bytes.toString(result.getRow()))
            println(Bytes.toString(result.getValue(Bytes.toBytes("info"), Bytes.toBytes("a"))))
            printRowKey(rs)
        }
    }

    def main(args: Array[String]) {
        val conf = new SparkConf().setMaster("local[*]").setAppName("Server297").set("spark.driver.allowMultipleContexts", "true").set("spark.streaming.concurrentJobs", "2")
//        val sc = new SparkContext(conf)
        val ssc = new StreamingContext(conf, Seconds(5))
        val kafkaStream = KafkaUtils.createStream(ssc, "localhost:2181","spark-streaming-consumer-group", Map("yelp" -> 5))
//        val kafkaStream2 = KafkaUtils.createStream(ssc, "localhost:2181","spark-streaming-consumer-group", Map("yelp2" -> 5))


//      val kafkaParams = Map("metadata.broker.list" -> "localhost:2181")
//      val topics = Set("yelp")
//
//      val kafkaStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topics)

        val lines = kafkaStream.map(_._2)
        val words = lines.flatMap(_.split(" "))

      //        val hconf = HBaseConfiguration.create()
//        hconf.set("hbase.zookeeper.property.clientPort", "2084")
//        hconf.set("hbase.zookeeper.quorum", "datanode1, datanode2, datanode0")
//        hconf.set(TableInputFormat.INPUT_TABLE, "restaurant")
//
//        val jobConf = new JobConf(hconf, this.getClass)
//        jobConf.setOutputFormat(classOf[TableOutputFormat])
//        jobConf.set(TableOutputFormat.OUTPUT_TABLE, "restaurant")
//
//        val dbRDD = sc.newAPIHadoopRDD(hconf, classOf[TableInputFormat],
//            classOf[ImmutableBytesWritable],
//            classOf[Result])
//
//        dbRDD.cache()
        val hconf = HBaseConfiguration.create()
        val connection = ConnectionFactory.createConnection(hconf)
        val admin = connection.getAdmin()
        val listtables = admin.listTables()
        listtables.foreach(println)
        val table = connection.getTable(TableName.valueOf("vegas"))
        val table2 = connection.getTable(TableName.valueOf("pheonix"))
        val table3 = connection.getTable(TableName.valueOf("charlotte"))
        val table4 = connection.getTable(TableName.valueOf("madison"))
        val table5 = connection.getTable(TableName.valueOf("pittsburgh"))
        val table6 = connection.getTable(TableName.valueOf("montreal"))

//        val result = table.get(new Get(Bytes.toBytes("fbc9fdefb5e1391c34abd4da2c88a13f")))
//        val value = Bytes.toString(result.getValue(Bytes.toBytes("data"), Bytes.toBytes("content")))
//        println(value)

        var count:Int = 0

        words.foreachRDD ( rdd => {
            for (item <- rdd.collect()) {

//                println(item.toString)
//                println("count: " + count.toString)

//                val json = parse(item.toString)
//                val json = parse("{city1:tai,city2:gg}")
//                val c = json.extract[city]
//                println(c.city1)


                count += 1

                val rs = table.getScanner(new Scan())
                printRowKey(rs)
                rs.close()

//                saveRestaurantInfo(dbRDD.count(), item)
            }
        })



        sys.ShutdownHookThread {
            table.close()
            table2.close()
            ssc.stop(true, true)
            println("Application stopped")
        }

        ssc.start
        ssc.awaitTermination
    }
}
