package weathered.dataload

import akka.actor.{ActorLogging, Props, Actor, ActorSystem}
import com.typesafe.config.ConfigFactory
import java.io.{ByteArrayOutputStream, File}
import akka.serialization.SerializationExtension
import akka.zeromq._
import org.apache.log4j.Logger
import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.io.Input
import akka.zeromq.Listener
import akka.zeromq.Bind

/**
 * ZeroMQ version of the old LoadISDFile, to operate in tandem with ISDFileDataLoader
 *
 * @author Christopher Childs
 * @version 1.0, 7/21/12
 */

object ISDLineLoader {

  def main(args:Array[String]) {
    val system = ActorSystem("isd-line-reader", ConfigFactory.parseFile(new File(args(0))))

    // TODO: needs to be configurable


    val subscriber = system.newSocket(SocketType.Sub, Listener(system.actorOf(Props[ISDLineProcessor],
      name = "lineprocessor")), Connect("tcp://127.0.0.1:6001"), Identity(("isdlineloader" + System.nanoTime()).getBytes), SubscribeAll)
  }

}

class ISDLineProcessor extends Actor {
  val log = Logger.getLogger(this.getClass)
  val kryo = new Kryo()
  var lines = 0
  val start = System.nanoTime()

  protected def receive = {
    case m:ZMQMessage  => {
      val input = new Input(m.payload(1))
      val line = kryo.readObject(input, classOf[LineToIndex])
      lines += 1
      if (lines % 1000 == 0) {
        val elapsed = System.nanoTime() - start
        val lps = (lines.toDouble / (elapsed.toDouble / 1000000000.toDouble))
        log.info("lines received: " + lines + ", lines per second: " + lps)
      }
    }

  }
}