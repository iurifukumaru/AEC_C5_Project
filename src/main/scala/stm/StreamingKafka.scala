package stm

import org.apache.hadoop.fs.Path
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object StreamingKafka extends App with HDFS {

  if(fs.delete(new Path(s"$uri/user/winter2020/iuri/course5/project"),true))
    println("Folder 'course 5 - project' deleted before Instantiate!")

  fs.mkdirs(new Path(s"$uri/user/winter2020/iuri/course5/project/trips"))
  fs.mkdirs(new Path(s"$uri/user/winter2020/iuri/course5/project/calendar_dates"))
  fs.mkdirs(new Path(s"$uri/user/winter2020/iuri/course5/project/frequencies"))
  fs.mkdirs(new Path(s"$uri/user/winter2020/iuri/course5/project/enriched_stop_time"))

  fs.copyFromLocalFile(new Path("file:///home/iuri/Documents/Course5/trips.txt"),
    new Path(s"$uri/user/winter2020/iuri/course5/project/trips/trips.txt"))
  fs.copyFromLocalFile(new Path("file:///home/iuri/Documents/Course5/calendar_dates.txt"),
    new Path(s"$uri/user/winter2020/iuri/course5/project/calendar_dates/calendar_dates.txt"))
  fs.copyFromLocalFile(new Path("file:///home/iuri/Documents/Course5/frequencies.txt"),
    new Path(s"$uri/user/winter2020/iuri/course5/project/frequencies/frequencies.txt"))

  val spark = SparkSession.builder().appName("Course 5 Project").master("local[*]").getOrCreate()

  val tripsFile = "/user/winter2020/iuri/course5/project/trips/trips.txt"
  val calendarFile = "/user/winter2020/iuri/course5/project/calendar_dates/calendar_dates.txt"
  val frequenciesFile = "/user/winter2020/iuri/course5/project/frequencies/frequencies.txt"

  val tripsDf = spark.read.option("header", "true").option("inferschema", "true").csv(tripsFile)
  val calendarDf = spark.read.option("header", "true").option("inferschema", "true").csv(calendarFile)
  val frequenciesDf = spark.read.option("header", "true").option("inferschema", "true").csv(frequenciesFile)

  tripsDf.createOrReplaceTempView("trips")
  calendarDf.createOrReplaceTempView("calendar")
  frequenciesDf.createOrReplaceTempView("frequencies")
  val enrichedTrip = spark.sql(
    """
      |SELECT t.route_id, t.service_id, t.trip_id, t.trip_headsign, t.wheelchair_accessible,
      |c.date, c.exception_type,
      |f.start_time, f.end_time, f.headway_secs
      |FROM trips t
      |LEFT JOIN calendar c ON (t.service_id = c.service_id)
      |LEFT JOIN frequencies f ON (t.trip_id = f.trip_id)
      |""".stripMargin)

  val enrichedTripDf = enrichedTrip.toDF()

  val ssc = new StreamingContext(spark.sparkContext, Seconds(10))

  val kafkaConfig = Map[String, String](
    ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "localhost:9092",
    ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer].getName,
    ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer].getName,
    ConsumerConfig.GROUP_ID_CONFIG -> "stop_times",
    ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "earliest",
    ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG -> "false"
  )

  val topic = "stop_times"
  val inStream: DStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](
    ssc,
    LocationStrategies.PreferConsistent,
    ConsumerStrategies.Subscribe[String, String](List(topic), kafkaConfig)
  )

  inStream.map(_.value()).foreachRDD(rdd => businessLogic(rdd))

  def businessLogic(rdd: RDD[String]) = {
    val stopTime: RDD[StopTime] = rdd.map(fromCsv => StopTime(fromCsv))
    import spark.implicits._
    val stopTimeDf = stopTime.toDF()
    enrichedTripDf.createOrReplaceTempView("enrichedtrip")
    stopTimeDf.createOrReplaceTempView("stoptime")
    val joinResult = spark.sql(
      """
      |SELECT et.route_id, et.service_id, et.trip_id, et.trip_headsign, wheelchair_accessible, date, exception_type,
      |start_time, end_time, headway_secs, arrival_time, departure_time, stop_id, stop_sequence
      |FROM enrichedtrip et JOIN stoptime ON et.trip_id = stoptime.trip_id
      |""".stripMargin)
    joinResult.write.mode(SaveMode.Append).csv("/user/winter2020/iuri/course5/project/enriched_stop_time")
  }

  ssc.start()
  ssc.awaitTermination()
}
