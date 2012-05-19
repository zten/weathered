package weathered;

import com.mongodb.casbah.Imports._
import java.io.FileInputStream
import org.apache.log4j.Logger
import java.util.GregorianCalendar
import java.io.File
import akka.actor._
import akka.routing.RoundRobinRouter

/**
 * Entry point for the most fantabulous ISD lite parsing and indexing app ever.
 * Or maybe it's not the most fantabulous.
 *
 */
object LoadISDFile {
  val log = Logger.getLogger(this.toString)

  def recursiveListFiles(f: File): Array[File] = {
    val these = f.listFiles
    these ++ these.filter(_.isDirectory).filter(_.getName.matches(".+\\.gz")).flatMap(recursiveListFiles)
  }

  def main(args:Array[String]) {
    if (args.length == 0) {
      println("need to specify a filename for an ISD lite folder")
      System.exit(1)
    }

    val system = ActorSystem("system")

    val listener = system.actorOf(Props[ListeningActor], name = "listener")

    val workers = system.actorOf(Props(new ISDIndexActor(listener)).withRouter(RoundRobinRouter(8)), name = "workers")

    recursiveListFiles(new File(args(0))).foreach(f => workers ! IndexFile(f))

  }
}

sealed trait IndexingCommand
case class IndexFile(file:File) extends IndexingCommand
case object IndexedAFile extends IndexingCommand

class ListeningActor extends Actor {
  var count:Int = 0
  val start = System.nanoTime()
  var elapsed:Long = 0

  protected def receive = {
    case command:IndexingCommand =>
      command match {
        case IndexedAFile => {
          count += 1
          elapsed = System.nanoTime() - start
          if ((count % 10) == 0) {
            println(count)
            println("avg files per second: " + (count.toDouble / (elapsed.toDouble / 1000000.toDouble)))
            println("elapsed time: " + (elapsed / 1000000) + " seconds")
          }

        }
      }
  }
}

class ISDIndexActor(val listener:ActorRef) extends Actor {
  val log = Logger.getLogger(this.toString)
  val db = MongoConnection()("weathered")
  val stations = db("stations")
  val coll = db("observations")

  protected def receive = {
    case command:IndexingCommand =>
      command match {
        case IndexFile(f) => {
          val nameComponents = f.getName.split("-")
          if (nameComponents.length != 3) {
            println("ISD file name should fit the format <USAF>-<WBAN>-<year>")
          } else {
            val usaf = nameComponents(0)
            val wban = nameComponents(1)
            val year = nameComponents(2)
            stations.findOne(MongoDBObject("usaf" -> usaf, "wban" -> wban)) match {
              case Some(x) => {
                val station = x
                val stationYearDoc = MongoDBObject.newBuilder

                stationYearDoc += "station" -> station
                stationYearDoc += "year" -> year
                stationYearDoc += "observations" -> io.Source.fromInputStream(
                  new FileInputStream(f)).getLines().map(line => {
                  val list = line.split("\\s+").map(s => Integer.valueOf(s))

                  if (list.length != 12) {
                    log.error("there should be exactly 12 observations, offending file: " + f.getName)
                    DBObject.empty
                  } else {
                    val docBuilder = MongoDBObject.newBuilder
                    // Forgot months start at 0...
                    val date = new GregorianCalendar(list(0), list(1) - 1, list(2), list(3), 0).getTime
                    docBuilder += "date" -> date
                    docBuilder += "airtemp" -> list(4)
                    docBuilder += "dewpointtemp" -> list(5)
                    docBuilder += "sealevelpressure" -> list(6)
                    docBuilder += "winddirection" -> list(7)
                    docBuilder += "windspeedrate" -> list(8)
                    docBuilder += "skycondition" -> list(9)
                    docBuilder += "liquidprecipdepth_hour" -> list(10)
                    docBuilder += "liquidprecipdepth_sixhour" -> list(11)

                    docBuilder.result()
                  }
                }).toList

                coll.save(stationYearDoc.result())
                listener ! IndexedAFile
              }
              case None => {
                println("Couldn't find a station with usaf " + usaf + " and wban " + wban)
              }
            }
          }
        }

    }
  }
}
