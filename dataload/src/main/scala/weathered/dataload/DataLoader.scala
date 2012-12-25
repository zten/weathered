package weathered.dataload

import com.mongodb.casbah.Imports._
import java.io.{EOFException, InputStream, FileInputStream, File}
import org.apache.log4j.Logger
import java.util
import akka.actor._
import akka.routing.RoundRobinRouter
import com.typesafe.config.ConfigFactory
import com.google.common.hash.{Hashing, HashCode}
import collection.mutable.ListBuffer
import util.concurrent.{TimeUnit, Delayed, DelayQueue}
import util.regex.Pattern
import util.zip.{ZipException, GZIPInputStream}
import java.nio.file.FileSystems
import com.mongodb.casbah.MongoURI
import collection.mutable
import com.twitter.ostrich.admin.{RuntimeEnvironment, Service}
import com.twitter.ostrich.admin.config.ServerConfig

/**
 * Service for loading ISD-Lite data into MongoDB. Designed to work with another program, such as wget,
 * mirroring the contents of the NOAA repository. This program will pick up changes as they're written to disk
 * and subsequently re-index them.
 *
 */
object DataLoader {
  val log = Logger.getLogger(this.toString)
  val ISD_FILE_PATTERN = Pattern.compile("(\\d{6})-(\\d{5})-(\\d{4}).*")
  val retry = new DelayQueue[DelayedFile]()

  implicit def enrichMongoDBObject(xs: DBObject) = {
    val upgraded = new WeatherObject()
    upgraded.putAll(xs)

    upgraded
  }

  def main(args: Array[String]) {
    val env = RuntimeEnvironment(this, args)
    val server = env.loadRuntimeConfig[DataLoader]()

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

      case Right(db) =>
        val workers = system.actorOf(Props(new ISDIndexActor(db)).withRouter(RoundRobinRouter(actors)).
          withDispatcher("custom-dispatch"), name = "workers")
        val dispatcherHelper = system.actorOf(Props(new IndexDispatcher(workers)), name = "dispatcher-helper")

        val path = FileSystems.getDefault.getPath(args(0))
        val watcher = new MonitorDirectory(path, ISD_FILE_PATTERN, dispatcherHelper)
        val watcherThread = new Thread(watcher)
        watcherThread.start()

        val requeueThread = new Thread(new RequeueAgent(dispatcherHelper))
        requeueThread.start()
    }

  }
}

class DataLoaderConfig extends ServerConfig[DataLoader] {

  def apply(runtime: RuntimeEnvironment) = {
    new DataLoader("")
  }
}

class DataLoader(val mongoDbUri:String) extends Service {
  private var actorSystem:ActorSystem = null

  def start() {
    val uri = MongoURI(mongoDbUri)
    uri.connectDB match {
      case Left(t) =>
        //log.error("Couldn't connect to db", t)
        System.exit(-1)
      case Right(db) =>

    }
  }

  def shutdown() {}
}

/**
 * Helper thread that provides a re-queuing mechanism if the file isn't immediately readable.
 *
 * @param indexer A reference to our dispatcher actor.
 */
class RequeueAgent(indexer:ActorRef) extends Runnable {
  def run() {
    while (true) {
      indexer ! IndexFile(DataLoader.retry.take().getFile)
    }
  }
}

class DelayedFile(f:File, delay:Long, unit:TimeUnit) extends Delayed {
  def getDelay(unit: TimeUnit) = {
    unit.convert(this.delay, this.unit)
  }

  def compareTo(o: Delayed) = {
    val diff = getDelay(TimeUnit.MICROSECONDS) - o.getDelay(TimeUnit.MICROSECONDS)
    if (diff < 0) {
      -1
    } else if (diff > 0) {
      1
    } else {
      0
    }
  }

  def getFile = f
}

sealed trait IndexingCommand

case class IndexFile(file: File) extends IndexingCommand

sealed trait IndexingResponse

case class IndexedAFile(file: File) extends IndexingResponse

/**
 * It was simpler to implement a dispatcher as another actor since this system
 * requires us to talk back to the dispatcher when work is finished. Of course,
 * this isn't robust, and requires the actors to not fail (otherwise files will get 'stuck').
 *
 * @param workers The actor worker pool that will do the work, if this actor determines it's okay to pass on a message.
 */
class IndexDispatcher(val workers:ActorRef) extends Actor {
  private val fileSet:mutable.Set[File] = mutable.Set()
  protected def receive = {
    case cmd:IndexingCommand =>
      cmd match {
        case IndexFile(f) => {
          if (fileSet.contains(f)) {
            // drop it on the floor
          } else {
            fileSet += f
            workers ! IndexFile(f)
          }
        }
      }

    case response:IndexingResponse =>
      response match {
        case IndexedAFile(f) => {
          fileSet -= f
        }
      }
  }
}

/**
 * Helper class for making weather objects comparable.
 */
class WeatherObject extends MongoDBObject {
  override def equals(that: Any) = {
    that match {
      case o:WeatherObject =>
        val us = this.getFullHash
        val them = o.getFullHash
        us.sameElements(them)
      case _:AnyRef =>
        false
    }
  }

  override def hashCode() = {
    getHasher.asInt()
  }

  def getFullHash = {
    getHasher.asBytes()
  }

  def getDate = {
    this.getAs[util.Date]("date").get
  }

  private def getHasher = {
    val date = this.getAs[util.Date]("date").get
    val airtemp = this.getAs[Int]("airtemp").get
    val dewpointtemp = this.getAs[Int]("dewpointtemp").get
    val sealevelpressure = this.getAs[Int]("sealevelpressure").get
    val winddirection = this.getAs[Int]("winddirection").get
    val windspeedrate = this.getAs[Int]("windspeedrate").get
    val skycondition = this.getAs[Int]("skycondition").get
    val liquidprecipdepth_hour = this.getAs[Int]("liquidprecipdepth_hour").get
    val liquidprecipdepth_sixhour = this.getAs[Int]("liquidprecipdepth_sixhour").get

    getHash(date.getTime, airtemp, dewpointtemp, sealevelpressure, winddirection, windspeedrate, skycondition,
      liquidprecipdepth_hour, liquidprecipdepth_sixhour)
  }

  private def getHash(date:Long, airtemp:Int, dewpointtemp:Int, sealevelpressure:Int, winddirection:Int,
                      windspeedrate:Int, skycondition:Int, liquidprecipdepth_hour:Int,
                      liquidprecipdepth_sixhour:Int):HashCode = {
    val hashCode = Hashing.sha512().newHasher()
    hashCode.putLong(date).putInt(airtemp).
      putInt(dewpointtemp).putInt(sealevelpressure).putInt(winddirection).putInt(windspeedrate).
      putInt(skycondition).putInt(liquidprecipdepth_hour).putInt(liquidprecipdepth_sixhour)
    hashCode.hash()
  }
}

/**
 * Implementation of actual indexing process.
 *
 * @param db The MongoDB database to use.
 */
class ISDIndexActor(val db: MongoDB) extends Actor {
  val log = Logger.getLogger(this.toString)
  val stations = db("stations")
  val coll = db("observations")
  log.info(self.toString() + " created")

  private def getDBObjects(is:InputStream):List[WeatherObject] = {
    import DataLoader.enrichMongoDBObject

    val docs = ListBuffer[WeatherObject]()

    io.Source.fromInputStream(is).getLines().foreach(line => {
      val list = line.split("\\s+").map(s => Integer.valueOf(s))

      if (list.length == 12) {
        val date = new util.GregorianCalendar(list(0).toInt, list(1) - 1, list(2), list(3), 0).getTime

        val docBuilder = MongoDBObject.newBuilder
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
    })

    docs.toList
  }

  protected def receive = {
    case command: IndexingCommand =>
      command match {
        case IndexFile(f) => {
          val matcher = DataLoader.ISD_FILE_PATTERN.matcher(f.getName)
          if (!matcher.matches()) {
            log.error("tried processing " + f.getName + " which doesn't fit ISD filename format")
          } else {
            val usaf = matcher.group(1)
            val wban = matcher.group(2)
            val year = matcher.group(3)
            stations.findOne(MongoDBObject("usaf" -> usaf, "wban" -> wban)) match {
              case Some(station) => {
                var tempIs = new FileInputStream(f)
                var is:InputStream = null
                try {
                  // Yeah, this is pretty insane, but it's just to test that we can read
                  // the file in question before attempting to read it for real.
                  is = new GZIPInputStream(tempIs)
                  is.read()
                  is.close()
                  tempIs = new FileInputStream(f)
                  is = new GZIPInputStream(tempIs)

                  val pre = System.nanoTime()

                  coll.findOne(MongoDBObject("station" -> station, "year" -> year)) match {
                    case Some(observations) =>
                      val existing = mutable.Set[WeatherObject]()
                      for (obj <- observations.getAs[MongoDBList]("observations").get) {
                        existing += DataLoader.enrichMongoDBObject(obj.asInstanceOf[DBObject])
                      }

                      val allObservations = getDBObjects(is)
                      val difference = allObservations.toSet -- existing
                      val newObservations = difference.size

                      // date sort order was destroyed, frustratingly.
                      val toSave = difference.toList.sortBy(k => k.getDate)

                      toSave.foreach(x => coll.update(observations, $push("observations" -> x)))

                      val post = (System.nanoTime() - pre)/1000000
                      log.info("Updated record for USAF %s WBAN %s for year %s in %dms (%d new observations)".
                        format(usaf, wban, year, post, newObservations))

                    case None =>
                      val builder = MongoDBObject.newBuilder
                      builder += "station" -> station
                      builder += "year" -> year
                      val observations = getDBObjects(is)
                      builder += "observations" -> observations
                      coll.save(builder.result())
                      val post = (System.nanoTime() - pre)/1000000
                      log.info("Created record for USAF %s WBAN %s for year %s in %dms (%d observations)".
                        format(usaf, wban, year, post, observations.size))
                  }
                } catch {
                  case ze:ZipException => {
                    DataLoader.retry.offer(new DelayedFile(f, 5, TimeUnit.SECONDS))
                  }
                  case eof:EOFException => {
                    DataLoader.retry.offer(new DelayedFile(f, 5, TimeUnit.SECONDS))
                  }
                } finally {
                  if (is != null) {
                    is.close()
                  }
                }
              }
              case None => {
                log.error("Couldn't find a station with usaf " + usaf + " and wban " + wban)
              }
            }
          }

          sender ! IndexedAFile(f)
        }
      }
  }
}
