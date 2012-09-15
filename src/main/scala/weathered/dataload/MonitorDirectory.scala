package weathered.dataload

import java.nio.file.{Paths, StandardWatchEventKinds, Path}
import java.util.regex.Pattern
import akka.actor.ActorRef

import scala.collection.JavaConversions._
import org.apache.log4j.Logger
import org.apache.commons.io.FileUtils
import org.apache.commons.io.filefilter.{TrueFileFilter, FalseFileFilter}

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
      val directories = FileUtils.listFilesAndDirs(path.toFile, FalseFileFilter.FALSE, TrueFileFilter.TRUE)
      for (val d <- directories) {
        log.info("Registering path " + d.toString)
        d.toPath.register(watcher, StandardWatchEventKinds.ENTRY_CREATE, StandardWatchEventKinds.ENTRY_MODIFY)
      }

      while (true) {
        val key = watcher.take()
        val basePath = key.watchable() match {
          case p:Path => p
        }

        key.pollEvents().foreach(
          e => {
            if (e.context() != null) {
              e.context() match {
                case p:Path =>
                  val fullPath = Paths.get(basePath.toString, p.toString)
                  log.info("Event triggered for path " + fullPath.toString + "; event: " + e.kind().toString)
                  val file = fullPath.toFile
                  if (file.isDirectory) {
                    if (e.kind().equals(StandardWatchEventKinds.ENTRY_CREATE)) {
                      log.info("Registering path " + fullPath.toString)
                      fullPath.register(watcher, StandardWatchEventKinds.ENTRY_CREATE, StandardWatchEventKinds.ENTRY_MODIFY)
                    }
                  } else {
                    val m = pattern.matcher(file.getName)
                    if (m.matches()) {
                      indexer ! IndexFile(file)
                    }
                  }

              }
            } else {
              log.warn("event context null; event kind: " + e.kind().toString)
            }

          }
        )

        key.reset()
      }
    } catch {
      case ex:Exception => {
        log.error(ex)
        log.error(ex.getStackTraceString)
      }
    }
  }
}