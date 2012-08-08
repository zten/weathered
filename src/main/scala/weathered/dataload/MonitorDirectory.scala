package weathered.dataload

import java.nio.file.{StandardWatchEventKinds, Path}
import java.util.regex.Pattern
import akka.actor.ActorRef

import scala.collection.JavaConversions._
import org.apache.log4j.Logger

/**
 * Monitors a directory for changes so that we can start the indexing process and handle the changes.
 *
 * @author Christopher Childs
 * @version 1.0, 8/8/12
 */

class MonitorDirectory(val path:Path, val pattern:Pattern, val indexer:ActorRef) extends Runnable {
  val log = Logger.getLogger(this.getClass)

  def run() {
    try {
      val watcher = path.getFileSystem.newWatchService()
      path.register(watcher, StandardWatchEventKinds.ENTRY_CREATE, StandardWatchEventKinds.ENTRY_MODIFY)

      while (true) {
        val key = watcher.take()
        key.pollEvents().foreach(
          e =>
            e.context() match {
              case path:Path =>
                val file = path.toFile
                val m = pattern.matcher(file.getName)
                if (m.matches()) {
                  indexer ! IndexFile(path.toFile)
                }
            }
        )
      }
    } catch {
      case ex:Exception => log.error(ex)
    }
  }
}