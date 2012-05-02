package weathered;

import com.mongodb.casbah.Imports._
import java.io.FileInputStream
import org.apache.log4j.Logger
import java.util.GregorianCalendar

/**
 * Entry point for the most fantabulous ISD lite parsing and indexing app ever.
 * Or maybe it's not the most fantabulous.
 *
 */
object LoadISDFile {
  val log = Logger.getLogger(this.toString)

  def main(args:Array[String]) {
    if (args.length == 0) {
      println("need to specify a filename for an ISD lite file")
      System.exit(1)
    }

    val nameComponents = args(0).split("-")
    if (nameComponents.length != 3) {
      println("ISD file name should fit the format <USAF>-<WBAN>-<year>")
      System.exit(1)
    }

    val db = MongoConnection()("weathered")
    val stations = db("stations")
    val coll = db("observations" + nameComponents(2))

    val usaf = nameComponents(0)
    val wban = nameComponents(1)

    val station = stations.findOne(MongoDBObject("usaf" -> usaf, "wban" -> wban)) match {
      case Some(x) => x
      case None => {
        println("Couldn't find a station with usaf " + usaf + " and wban " + wban)
        System.exit(1)
      }
    }

    io.Source.fromInputStream(new FileInputStream(args(0))).getLines().foreach(line => {
      val list = line.split("\\s+").map(s => Integer.valueOf(s))

      if (list.length != 12) {
        log.error("there should be exactly 12 observations")
        System.exit(1)
      }


      val docBuilder = MongoDBObject.newBuilder
      val date = new GregorianCalendar(list(0), list(1) - 1, list(2), list(3), 0).getTime
      docBuilder += "station" -> station
      docBuilder += "date" -> date
      docBuilder += "airtemp" -> list(4)
      docBuilder += "dewpointtemp" -> list(5)
      docBuilder += "sealevelpressure" -> list(6)
      docBuilder += "winddirection" -> list(7)
      docBuilder += "windspeedrate" -> list(8)
      docBuilder += "skycondition" -> list(9)
      docBuilder += "liquidprecipdepth_hour" -> list(10)
      docBuilder += "liquidprecipdepth_sixhour" -> list(11)

      coll.save(docBuilder.result())
    })

    log.info("observations recorded from input file")
  }
}
