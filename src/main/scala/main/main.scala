package main

import com.datastax.spark.connector.cql.CassandraConnector
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.log4j.{Logger, Level}

/**
 * Created by plamb on 2/27/16.
 */
object main {
//Case class for handling log events
  case class LogEvent(ip:String, timestamp:String, requestPage:String, responseCode:Int, responseSize:Int, userAgent:String)
//Function to parse text logs into the case class
  def parseLogEvent(event: String): LogEvent = {
    val LogPattern = """^([\d.]+) (\S+) (\S+) \[(.*)\] \"([^\s]+) (/[^\s]*) HTTP/[^\s]+\" (\d{3}) (\d+) \"([^\"]+)\" \"([^\"]+)\"$""".r
    val m = LogPattern.findAllIn(event)
    if (m.nonEmpty)
      new LogEvent(m.group(1), m.group(4), m.group(6), m.group(7).toInt, m.group(8).toInt, m.group(10))
    else
      null
  }

  //val dateFormat = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss Z")

  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)

def main(args: Array[String]) {
  val conf = new SparkConf(true)
    .setAppName("SparkWebLogNew")
    .setMaster("local[4]")
    .set("spark.cassandra.connection.host", "127.0.0.1")

  CassandraConnector(conf).withSessionDo { session =>
    session.execute(s"DROP KEYSPACE IF EXISTS ipAddresses")
    session.execute(s"CREATE KEYSPACE IF NOT EXISTS ipAddresses WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1 }")
    session.execute(s"CREATE TABLE ipAddresses.timeOnPage (IP text PRIMARY KEY, page map<text, bigint>)")
  }

  val sc = new SparkContext(conf)
  val ssc = new StreamingContext(sc, Seconds(5))

  //Kafka Settings
  val events = KafkaUtils.createStream(ssc,"localhost:2181","sparkFetcher",Map("apache"->2))
    .map(_._2) // grab just the web log from the stream

  val parseLogs = events.flatMap(_.split("\n")).map(parseLogEvent)

  parseLogs.print()


  ssc.start()
  ssc.awaitTermination()

}
}
