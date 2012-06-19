package weathered

import org.apache.log4j.Logger
import java.io.{FileInputStream, File}
import backtype.storm.topology.base.{BaseRichBolt, BaseRichSpout}
import backtype.storm.topology.{TopologyBuilder, OutputFieldsDeclarer}
import java.util
import backtype.storm.task.{OutputCollector, TopologyContext}
import backtype.storm.spout.SpoutOutputCollector
import backtype.storm.tuple.{Values, Tuple, Fields}
import backtype.storm.{LocalCluster, Config}
import util.concurrent.LinkedBlockingQueue
import backtype.storm.utils.Utils
import com.mongodb.casbah.commons.MongoDBObject
import com.mongodb.casbah.Imports._

/**
 * A shot at using Storm to stream data to workers, instead of the actor system in LoadISDFile.
 *
 * @author Christopher Childs
 * @version 1.0, 6/18/12
 */

object LoadISDStorm {
  val log = Logger.getLogger(this.toString)
  def main(args:Array[String]) {
    val dir = System.getProperty("weathered.dataDir")
    if (dir == null) {
      log.error("need to specify a filename for an ISD lite folder")
      System.exit(1)
    }

    log.info("Reading from " + dir)

    val builder = new TopologyBuilder()
    builder.setSpout("observations", new ObservationProducer(), 1)
    builder.setBolt("dbDriver", new MongoUpdateBolt(), 4).shuffleGrouping("observations")
    builder.setBolt("counter", new MonitorBolt(), 1).globalGrouping("dbDriver")


    val config = new Config()
    //config.setDebug(true)
    config.put("directory", dir)

    val cluster = new LocalCluster()
    cluster.submitTopology("reader", config, builder.createTopology())

    Utils.sleep(180000)
    cluster.shutdown()

  }

}

class Observation(val usaf:String, val wban:String, val line:String, val stationYear:DBObject)

object ObservationProducer {
  private val log = Logger.getLogger(this.toString)
}

class ObservationProducer extends BaseRichSpout {
  private var _collector:SpoutOutputCollector = null
  val queue = new LinkedBlockingQueue[Observation](100)
  var thread:Thread = null

  def open(conf: util.Map[_, _], context: TopologyContext, collector: SpoutOutputCollector) {
    _collector = collector
    thread = new Thread(new Queuer(MongoConnection()("weathered"), new File(conf.get("directory").asInstanceOf[String]), queue))
    thread.start()
  }

  def nextTuple() {
    val offering = queue.poll(1, util.concurrent.TimeUnit.SECONDS)
    if (offering == null) {
      ObservationProducer.log.info("couldn't poll")
      Utils.sleep(50)
    } else {
      val tmp = new Values()
      tmp.add(offering.usaf.asInstanceOf[Object])
      tmp.add(offering.wban.asInstanceOf[Object])
      tmp.add(offering.line.asInstanceOf[Object])
      tmp.add(offering.stationYear.asInstanceOf[Object])
      _collector.emit(tmp)
    }

  }

  def declareOutputFields(declarer: OutputFieldsDeclarer) {
    declarer.declare(new Fields("usaf", "wban", "line", "stationYear"))
  }
}

object Queuer {
  private val log = Logger.getLogger(this.toString)
}

class Queuer(val db:MongoDB, val dir: File, val queue:LinkedBlockingQueue[Observation]) extends Runnable {
  private var stations:MongoCollection = null
  private var coll:MongoCollection = null

  def run() {
    stations = db("stations")
    coll = db("observations")
    while (true) {
      Helper.recursiveListFiles(dir).filter(_.getName.matches("\\d{6}-\\d{5}-\\d{4}")).foreach(f => enqueue(f))
    }
  }

  def enqueue(file: File) {
    val nameComponents = file.getName.split("-")
    if (nameComponents.length != 3) {
      Queuer.log.error("ISD file name should fit the format <USAF>-<WBAN>-<year>")
    } else {
      val usaf = nameComponents(0)
      val wban = nameComponents(1)
      val year = nameComponents(2)

      val idobject = MongoDBObject("_id" -> 1)
      val station = MongoDBObject("usaf" -> usaf, "wban" -> wban)

      val stationYear = {
        val toFind = MongoDBObject("station" -> station)
        coll.findOne(toFind, idobject) match {
          case Some(stationYearExists) =>
            toFind
          case None =>
            val stationYear = MongoDBObject.newBuilder
            stationYear += "station" -> stations.findOne(station)
            stationYear += "year" -> year
            val res = stationYear.result()
            coll.save(res)
            res
        }
      }


      io.Source.fromInputStream(new FileInputStream(file)).getLines().foreach(line => {
        queue.put(new Observation(usaf, wban, line, stationYear))
      })

    }

  }

}

object MongoUpdateBolt {
  private val log = Logger.getLogger(this.toString)
}

class MongoUpdateBolt extends BaseRichBolt {
  // TODO: Move this out to some configuration. There's a nice class for it, called Config.
  @transient
  private var db:MongoDB = null
  @transient
  private var stations:MongoCollection = null
  @transient
  private var coll:MongoCollection = null

  private var _collector:OutputCollector = null

  override def prepare(p1: util.Map[_, _], p2: TopologyContext, collector: OutputCollector) {
    db = MongoConnection()("weathered")
    stations = db("stations")
    coll = db("observations")
    _collector = collector
  }

  override def execute(tuple: Tuple) {
    val line = tuple.getString(2)
    val list = line.split("\\s+").map(s => Integer.valueOf(s))
    if (list.length != 12) {
      MongoUpdateBolt.log.error("received invalid input: %s".format(line))
    } else {
      doUpdate(tuple.getValue(3).asInstanceOf[DBObject], list)
      _collector.emit(new Values(""))
    }

  }

  def doUpdate(stationYear: MongoDBObject, list:Array[java.lang.Integer]) {
    val docBuilder = MongoDBObject.newBuilder
    // Forgot months start at 0...
    val date = new util.GregorianCalendar(list(0), list(1) - 1, list(2), list(3), 0).getTime
    docBuilder += "date" -> date
    docBuilder += "airtemp" -> list(4)
    docBuilder += "dewpointtemp" -> list(5)
    docBuilder += "sealevelpressure" -> list(6)
    docBuilder += "winddirection" -> list(7)
    docBuilder += "windspeedrate" -> list(8)
    docBuilder += "skycondition" -> list(9)
    docBuilder += "liquidprecipdepth_hour" -> list(10)
    docBuilder += "liquidprecipdepth_sixhour" -> list(11)

    // IntelliJ Scala plugin will report a highlight error here and there isn't one.
    val toPush = $push("observations" -> docBuilder.result())

    coll.update(stationYear, toPush)
  }


  def declareOutputFields(declarer: OutputFieldsDeclarer) {
    declarer.declare(new Fields("dummy"))
  }

}

class MonitorBolt extends BaseRichBolt {
  private var startTime:Long = 0
  private var count:Long = 0
  private var elapsed:Long = 0

  def prepare(p1: util.Map[_, _], p2: TopologyContext, p3: OutputCollector) {
    startTime = System.nanoTime()
  }

  def execute(p1: Tuple) {
    count += 1
    elapsed = System.nanoTime() - startTime
    if ((count % 1000) == 0) {
      println(count + " lines processed, avg lines per second: " +
        (count.toDouble / (elapsed.toDouble / 1000000000.toDouble)))
      println("elapsed time: " + (elapsed / 1000000000) + " seconds")
    }
  }

  def declareOutputFields(p1: OutputFieldsDeclarer) {

  }
}