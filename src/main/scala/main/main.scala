package main

import java.text.SimpleDateFormat

import com.datastax.spark.connector.cql.CassandraConnector
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{StateSpec, Seconds, StreamingContext}
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.log4j.{Logger, Level}

/**
 * Created by plamb on 2/27/16.
 */
object main {
  val dateFormat = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss Z")

//Case classes for handling log events
  case class LogEvent(ip:String, timestamp:Long, requestPage:String, responseCode:Int, responseSize:Int, userAgent:String)

//Function to parse text logs into the case class
  def parseLogEvent(event: String): LogEvent = {
    val LogPattern = """^([\d.]+) (\S+) (\S+) \[(.*)\] \"([^\s]+) (/[^\s]*) HTTP/[^\s]+\" (\d{3}) (\d+) \"([^\"]+)\" \"([^\"]+)\"$""".r
    val m = LogPattern.findAllIn(event)
    if (m.nonEmpty)
      new LogEvent(m.group(1), dateFormat.parse(m.group(4)).getTime(), m.group(6), m.group(7).toInt, m.group(8).toInt, m.group(10))
    else
      null
  }

  //Function passed to UpdateStateByKey. Using an IP as a key, accumulates a list of pages hit and times on page.
  def updateValues(
                    newValues: Seq[(String, Long, Long)],
                    currentValue: Option[Seq[(String, Long, Long)]]
                    ):
                    Option[Seq[(String, Long, Long)]] = {

    Some(currentValue.getOrElse(Seq.empty) ++ newValues)
  }


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

  //Parse the raw logs and filter for the pages we care about. Return k/v pairs of sessions
  val parseLogs = events.flatMap(_.split("\n")).map(parseLogEvent)
    .filter(
      logevent => logevent.requestPage.endsWith(".html") ||
                  logevent.requestPage.endsWith(".php") ||
                  logevent.requestPage.equals("/")
    ).map{
    event => (event.ip, (event.requestPage, event.timestamp, event.timestamp))
  }

  ssc.checkpoint("/home/plamb/Coding/SnappyData/My_Projects/misc/sparkweblognew-checkpoint")

  //Combine the keys and group pages times into a Map via the pages themselves.
  val ipToPage = parseLogs.updateStateByKey(updateValues).map{event => (event._1, event._2.groupBy(_._1))}

  val combineTimes = ipToPage.map{session =>

    val lol = session._2.mapValues{
          case Nil => None
          case (_, time1, time2) :: tail => //combineTimes(tail, time1, time2)
            Some(tail.foldLeft((time1, time2)) {
              case ((startTime, nextStartTime), (_, endTime, nextEndTime)) =>
                (startTime min nextStartTime, endTime max nextEndTime)
            })
    }

    (session._1, lol)

  }


//    .mapValues{
//    case Nil => None
//    case (_, time1, time2) :: tail => //combineTimes(tail, time1, time2)
//      Some(tail.foldLeft((time1, time2)) {
//        case ((startTime, nextStartTime), (_, endTime, nextEndTime)) =>
//          (startTime min nextStartTime, endTime max nextEndTime)
//      })
//  }}


  ipToPage.print()

//  def combineTimes(timesListTail: List[(String, Long, Long)], time1: Long, time2: Long) = {
//    timesListTail.foldLeft((time1, time2)) =>
//      case ((startTime, nextStartTime), (_, endTime, nextEndTime)) =>
//        (startTime.  nextStartTime, endTime max nextEndTime)
//  }

  ssc.start()
  ssc.awaitTermination()

}
}
