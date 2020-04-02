package ksb.csle.didentification.exceptions

import scala.language.reflectiveCalls

/**
 * This object handles the exception caused by processing data frame.
 */
object AnonymityExceptionHandler {
  
  def apply[R <: { def cache(): R}]
      (dataframe: => R)
      (f: R => R) = {
    var df: Option[R] = None
    try {
      df = Some(dataframe)
      f(df.get.cache())
    } catch {
      case others: Throwable => {
        others.printStackTrace()
        df.get.cache()
      }
    }
  }
    
}