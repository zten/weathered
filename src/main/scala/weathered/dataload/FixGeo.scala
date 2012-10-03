package weathered.dataload

import org.apache.log4j.Logger
import java.io.File
import java.util
import com.mongodb.casbah.MongoURI
import com.mongodb.casbah.Imports._
import scala.Left
import scala.Right

/**
 * I screwed up and needed a way to reload geo data without having to recreate stations.
 *
 * @author Christopher Childs
 * @version 1.0, 10/2/12
 */
object FixGeo {
  import LoadStations.parseNumber

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
            val lat = parseNumber(arr(7), 1000)
            val lon = parseNumber(arr(8), 1000)
            if ((lat >= -180 && lat <= 180) && (lon >= -180 && lon <= 180)) {
              coll.update(MongoDBObject("usaf" -> arr(0), "wban" -> arr(1)),
                MongoDBObject("$set" -> ("location" -> (lat, lon))))
            }
          }

          row += 1
        })

        log.info("stations updated")
    }
  }

}
