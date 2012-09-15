package weathered.dataload

import com.mongodb.casbah.Imports._
import java.io.{InputStream, FileInputStream, File}
import org.apache.log4j.Logger
import java.util
import akka.actor._
import akka.routing.RoundRobinRouter
import com.typesafe.config.ConfigFactory
import com.google.common.hash.{Hashing, HashCode}
import collection.mutable.{ArrayBuffer, ListBuffer}
import util.regex.Pattern
import util.zip.GZIPInputStream
import java.nio.file.FileSystems
import com.mongodb.casbah.MongoURI

/**
 * Entry point for the most fantabulous ISD lite parsing and indexing app ever.
 * Or maybe it's not the most fantabulous.
 *
 */
object LoadISDFile {
  val log = Logger.getLogger(this.toString)
  val ISD_FILE_PATTERN = Pattern.compile("(\\d{6})-(\\d{5})-(\\d{4}).*")

  def main(args: Array[String]) {
    if (args.length == 0 || args.length == 1) {
      println("need to specify a filename for an ISD lite folder, and a number of actors")
      System.exit(1)
    }

    val actors = args(1).toInt

    val system = ActorSystem("system", ConfigFactory.parseFile(new File("akka-application.conf")))

    log.info("Initializing with " + actors + " actor(s)")

    val config = new util.Properties()
    val resource = this.getClass.getClassLoader.getResourceAsStream("database.properties")
    if (resource != null) {
      // can give ourselves the opportunity to just pass the relevant properties as a JVM arg
      config.load(resource)
    }

    val uri = MongoURI(config.getProperty("mongodb.uri", ""))

    uri.connectDB match {
      case Left(t) =>
        log.error("Couldn't connect to db", t)
        System.exit(-1)
      case Right(db) =>
        val workers = system.actorOf(Props(new ISDIndexActor(db)).withRouter(RoundRobinRouter(actors)).
          withDispatcher("custom-dispatch"), name = "workers")

        val path = FileSystems.getDefault.getPath(args(0))
        val watcher = new MonitorDirectory(path, ISD_FILE_PATTERN, workers)
        val thread = new Thread(watcher)
        thread.start()
    }

  }
}

sealed trait IndexingCommand

case class IndexFile(file: File) extends IndexingCommand

case object IndexedAFile extends IndexingCommand

class ISDIndexActor(val db: MongoDB) extends Actor {
  val log = Logger.getLogger(this.toString)
  val stations = db("stations")
  val coll = db("observations")
  log.info(self.toString() + " created")

  protected def receive = {
    case command: IndexingCommand =>
      command match {
        case IndexFile(f) => {
          val matcher = LoadISDFile.ISD_FILE_PATTERN.matcher(f.getName)
          if (!matcher.matches()) {
            log.error("tried processing " + f.getName + " which doesn't fit ISD filename format")
          } else {
            val usaf = matcher.group(1)
            val wban = matcher.group(2)
            val year = matcher.group(3)
            stations.findOne(MongoDBObject("usaf" -> usaf, "wban" -> wban)) match {
              case Some(x) => {
                val station = x

                val docs = ListBuffer[DBObject]()

                var is: InputStream = null
                if (f.getName.endsWith(".gz")) {
                  is = new GZIPInputStream(new FileInputStream(f))
                } else {
                  is = new FileInputStream(f)
                }

                io.Source.fromInputStream(is).getLines().foreach(line => {
                  val list = line.split("\\s+").map(s => Integer.valueOf(s))

                  if (list.length != 12) {
                    log.error("there should be exactly 12 observations, offending file: " + f.getName)
                  } else {
                    // let's come up with a hash for the input so that we can check if we've recorded this observation
                    // already
                    val hashCode = Hashing.sha512().newHasher()

                    val date = new util.GregorianCalendar(list(0), list(1) - 1, list(2), list(3), 0).getTime

                    val airtemp = list(4)
                    val dewpointtemp = list(5)
                    val sealevelpressure = list(6)
                    val winddirection = list(7)
                    val windspeedrate = list(8)
                    val skycondition = list(9)
                    val liquidprecipdepth_hour = list(10)
                    val liquidprecipdepth_sixhour = list(11)

                    hashCode.putLong(date.getTime).putInt(airtemp).putInt(dewpointtemp).putInt(sealevelpressure).
                      putInt(winddirection).putInt(windspeedrate).putInt(skycondition).putInt(liquidprecipdepth_hour).
                      putInt(liquidprecipdepth_sixhour)

                    val code = hashCode.hash().asBytes()

                    val hashCheck = MongoDBObject.newBuilder
                    hashCheck += "_id" -> code

                    coll.findOne(hashCheck.result()) match {
                      case Some(observation) =>

                      case None =>
                        val docBuilder = MongoDBObject.newBuilder
                        docBuilder += "_id" -> code
                        docBuilder += "station" -> station
                        docBuilder += "year" -> year
                        docBuilder += "date" -> date
                        docBuilder += "airtemp" -> list(4)
                        docBuilder += "dewpointtemp" -> list(5)
                        docBuilder += "sealevelpressure" -> list(6)
                        docBuilder += "winddirection" -> list(7)
                        docBuilder += "windspeedrate" -> list(8)
                        docBuilder += "skycondition" -> list(9)
                        docBuilder += "liquidprecipdepth_hour" -> list(10)
                        docBuilder += "liquidprecipdepth_sixhour" -> list(11)

                        docs += docBuilder.result()
                    }
                  }
                })

                coll.insert(docs:_*)
              }
              case None => {
                log.error("Couldn't find a station with usaf " + usaf + " and wban " + wban)
              }
            }
          }
        }

        case IndexedAFile =>

      }
  }
}
