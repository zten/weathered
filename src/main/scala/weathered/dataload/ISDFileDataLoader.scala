package weathered.dataload

import akka.zeromq._
import akka.actor.ActorSystem
import com.typesafe.config.ConfigFactory
import java.io.{ByteArrayOutputStream, FileInputStream, File}
import weathered.Helper
import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.io.Output

/**
 * Will load files and push them off to my Actor bros in another process or whatever
 *
 * @author Christopher Childs
 * @version 1.0, 7/20/12
 */

object ISDFileDataLoader {

  def main(args:Array[String]) {
    // TODO: not robust, must invoke with exact arguments
    val system = ActorSystem("isd-line-publisher", ConfigFactory.parseFile(new File(args(0))))

    // TODO: needs to be configurable
    val pubSocket = system.newSocket(SocketType.Pub, Bind("tcp://127.0.0.1:6001"))

    val kryo = new Kryo()

    do {
      Helper.recursiveListFiles(new File(args(1))).foreach(f => {
        val nameComponents = f.getName.split("-")
        if (nameComponents.length != 3) {
          println("ISD file name should fit the format <USAF>-<WBAN>-<year>")
        } else {
          val usaf = nameComponents(0).toInt
          val wban = nameComponents(1).toInt
          val year = nameComponents(2).toInt


          io.Source.fromInputStream(new FileInputStream(f)).getLines().foreach(line => {
            val lineToIndex = new LineToIndex(usaf, wban, year, line)
            val output = new Output(new ByteArrayOutputStream())
            kryo.writeObject(output, lineToIndex)
            pubSocket ! ZMQMessage(Seq(Frame("observation.line"), Frame(output.toBytes)))
          })
        }
      })
    } while (true)

  }
}

class LineToIndex(var usaf:Int, var wban:Int, var year:Int, var line:String) {
  def this() = this(0, 0, 0, "")
}
