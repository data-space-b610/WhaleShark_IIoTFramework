package ksb.csle.didentification.exceptions

import java.io.{IOException, FileNotFoundException}
import scala.language.reflectiveCalls

/**
 * This object handles the exception caused by File related function.
 */
object IOExceptionHandler {
  
  def apply[R <: { def close(): Unit }, T]
      (resource: => R)
      (f: R => T) = {
    
    var res: Option[R] = None
    try {
      res = Some(resource)
      f(res.get)
    } catch {
      case ex: FileNotFoundException => ex.printStackTrace
      case ex: IOException => ex.printStackTrace
      case others: Throwable =>
        others.printStackTrace()
    } finally {
      if (res != None) res.get.close()
    }
  }
  
}