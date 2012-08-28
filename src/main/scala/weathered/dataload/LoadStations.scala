package weathered.dataload

import au.com.bytecode.opencsv.CSVReader
import java.io.{FileReader, File}
import com.mongodb.casbah.{MongoURI, MongoConnection}
import com.mongodb.casbah.Imports._
import org.apache.log4j.Logger
import java.util

/**
 * Reads CSV and creates collection of stations
 *
 * @author Christopher Childs
 * @version 1.0, 4/28/12
 */

object LoadStations {
  val log = Logger.getLogger(this.toString)

  def main(args:Array[String]) {
    if (args.length == 0) {
      println("Pass in the CSV file of stations to parse.")
      System.exit(1)
    }

    val inputFile = new File(args(0))
    if (!inputFile.exists()) {
      println("File " + inputFile.getCanonicalPath + " does not exist.")
      System.exit(1)
    }

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
        val coll = db("stations")

        var row = 0

        CSVWrapper.iterate(inputFile).foreach(arr => {
          if (row != 0) {
            // skip headers
            val station = MongoDBObject.newBuilder
            station += "usaf" -> arr(0)
            station += "wban" -> arr(1)
            station += "name" -> arr(2)
            station += "country" -> arr(3)
            station += "fipsCountry" -> arr(4)
            station += "state" -> arr(5)
            station += "callsign" -> arr(6)
            station += "location" -> (parseNumber(arr(7), 1000), parseNumber(arr(8), 1000))
            station += "elevation" -> arr(9)
            coll.save(station.result())
          }

          row += 1
        })

        log.info("stations loaded")
    }

  }

  def parseNumber(signedNum:String, divisor:Float):Float = {
    if (signedNum.length == 0) {
      0.0f
    } else {
      var neg = false
      if (signedNum.substring(0, 1).equals("-")) {
        neg = true
      }

      val num = signedNum.substring(1).toFloat
      val divided = num / divisor
      if (neg) {
        0.0f - divided
      } else {
        divided
      }
    }
  }

}

object CSVWrapper {
  def iterate(file:File) = {
    new CSVWrapper(file)
  }
}

class CSVWrapper(private var file:File) extends Iterator[Array[String]] {
  private var nextLine:Array[String] = null

  private val parser = new CSVReader(new FileReader(file))

  def hasNext = {
    nextLine = parser.readNext()
    if (nextLine == null) { false }
    else true
  }

  def next() = nextLine
}
