package weathered;

import com.mongodb.casbah.Imports._
import java.io.{FileInputStream, FileReader, BufferedReader}
import org.apache.log4j.Logger

/**
 * Entry point for the most fantabulous ISD lite parsing and indexing app ever.
 * Or maybe it's not the most fantabulous.
 *
 */
object Weathered {
  val log = Logger.getLogger("Weathered")

  def main(args:Array[String]) {
    val mongo = MongoConnection()

    val db = mongo("weatheredTest")
    val coll = db("observationTest")

    io.Source.fromInputStream(new FileInputStream(args(0))).getLines().foreach(line => {
      val list = line.split("\\s+").map(s => Integer.valueOf(s))

      if (list.length != 12) {
        log.error("there should be exactly 12 observations")
        System.exit(1)
      }

      val docBuilder = MongoDBObject.newBuilder
      docBuilder += "year" -> list(0)
      docBuilder += "month" -> list(1)
      docBuilder += "day" -> list(2)
      docBuilder += "hour" -> list(3)
      docBuilder += "airtemp" -> list(4)
      docBuilder += "dewpointtemp" -> list(5)
      docBuilder += "sealevelpressure" -> list(6)
      docBuilder += "winddirection" -> list(7)
      docBuilder += "windspeedrate" -> list(8)
      docBuilder += "skycondition" -> list(9)
      docBuilder += "liquidprecipdepth_hour" -> list(10)
      docBuilder += "liquidprecipdepth_sixhour" -> list(11)

      val doc = docBuilder.result()

      coll.save(doc)
    })

    log.info("observations recorded from input file")
  }
}
