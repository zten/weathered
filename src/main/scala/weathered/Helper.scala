package weathered

import java.io.File

/**
 * Reused utilities.
 *
 * @author Christopher Childs
 * @version 1.0, 6/18/12
 */

object Helper {
  def recursiveListFiles(f: File): Array[File] = {
    val these = f.listFiles
    these ++ these.filter(_.isDirectory).flatMap(recursiveListFiles)
  }

}
