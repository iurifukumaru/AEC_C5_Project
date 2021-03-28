package stm

object ExternalTableCheck extends App with HDFS {
  stmt.execute("SET hive.exec.dynamic.partition.mode = nonstrict")
  stmt.executeUpdate("""CREATE DATABASE IF NOT EXISTS winter2020_iuri""".stripMargin)
  stmt.executeUpdate("""DROP TABLE course5_ext_enriched_trip""".stripMargin)
  stmt.executeUpdate("""DROP TABLE course5_ext_enriched_stop_time""".stripMargin)
  stmt.executeUpdate(
    """CREATE EXTERNAL TABLE e_e_t (
      |route_id               INT,
      |service_id             STRING,
      |trip_id                STRING,
      |trip_headsign          STRING,
      |wheelchair_accessible  INT,
      |date                   STRING,
      |exception_type         INT,
      |start_time             STRING,
      |end_time               STRING,
      |headway_secs           INT
      |)
      |row format DELIMITED
      |fields TERMINATED BY ','
      |stored as textfile
      |LOCATION '/user/winter2020/iuri/course5/project/enriched_trip'
      |""".stripMargin
  )

  stmt.executeUpdate(
    """CREATE EXTERNAL TABLE e_e_s_t (
      |route_id               INT,
      |service_id             STRING,
      |trip_id                STRING,
      |trip_headsign          STRING,
      |wheelchair_accessible  INT,
      |date                   STRING,
      |exception_type         INT,
      |start_time             STRING,
      |end_time               STRING,
      |headway_secs           INT,
      |arrival_time           STRING,
      |departure_time         STRING,
      |stop_id                INT,
      |stop_sequence          INT
      |)
      |row format DELIMITED
      |fields TERMINATED BY ','
      |stored as textfile
      |LOCATION '/user/winter2020/iuri/course5/project/enriched_stop_time'
      |""".stripMargin
  )

}
