package utils

import org.scalatest.GivenWhenThen
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import utils.TestBase.{createTempDir, deleteRecursively}

import java.io.{File, IOException}

class TestBase extends AnyFlatSpec with GivenWhenThen with Matchers {
  def withTempDir(f: File => Unit): Unit = {
    val dir: File = createTempDir
    dir.mkdir()
    try f(dir) finally deleteRecursively(dir)
  }


}

object TestBase {

  val TEMP_DIR_ATTEMPTS = 10000

  def createTempDir: File = {
    val baseDir = new File(System.getProperty("java.io.tmpdir"))
    val baseName = System.currentTimeMillis + "-"
    for (counter <- 0 until TEMP_DIR_ATTEMPTS) {
      val tempDir = new File(baseDir, baseName + counter)
      if (tempDir.mkdir) return tempDir
    }
    throw new IllegalStateException("Failed to create directory within " +
      TEMP_DIR_ATTEMPTS +
      " attempts (tried " + baseName + "0 to " + baseName + (TEMP_DIR_ATTEMPTS - 1) + ')')
  }

  implicit class RichBoolean(bool: Boolean) {
    def toOption: Option[Boolean] = if (bool) Option(true) else None
  }

  /**
   * This function deletes a file or a directory with everything that's in it.
   */
  // scalastyle:off null
  private def deleteRecursively(file: File): Unit = {
    if (file != null) {
      try {
        if (file.isDirectory) {
          var savedIOException: IOException = null
          for (child <- listFiles(file)) {
            try {
              deleteRecursively(child)
            } catch {
              // In case of multiple exceptions, only last one will be thrown
              case ioe: IOException => savedIOException = ioe
            }
          }
          if (savedIOException != null) {
            throw savedIOException
          }
        }
      } finally {
        if (!file.delete()) {
          // Delete can also fail if the file simply did not exist
          if (file.exists()) {
            throw new IOException("Failed to delete: " + file.getAbsolutePath)
          }
        }
      }
    }
  }

  def listFiles(dir: String): Seq[File] = {
    listFiles(new File(dir))
  }

  def listFiles(dir: File): Seq[File] = {
    dir.exists.toOption.map(_ => Option(dir.listFiles()).map(_.toSeq).getOrElse {
      throw new IOException(s"Failed to list files for dir: $dir")
    }).getOrElse(Seq.empty[File])
  }

}