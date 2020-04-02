package ksb.csle.component.ingestion

import ksb.csle.common.base.UnitSpec

import org.apache.logging.log4j.scala.Logging
import org.scalatest.BeforeAndAfter

abstract class FurtherUnitSpec extends UnitSpec 
  with BeforeAndAfter 
  with Logging {
  
  @throws(classOf[Exception])
  def assertEqual(message: String, actual: Any, expected: Any): Unit = {
    logger.debug(s"$message: $actual == $expected")
    assert(actual == expected)
  }
  
  @throws(classOf[Exception])
  def assertEqual(actual: Any, expected: Any): Unit = {
    logger.debug(s"$actual == $expected")
    assert(actual == expected)
  }
  
  @throws(classOf[Exception])
  def assertNotEqual(actual: Any, expected: Any): Unit = {
    logger.debug(s"$actual != $expected")
    assert(actual != expected)
  }
  
//  def assertNoException(f: () => Unit): Unit = {
//    try {
//      f.apply()
//      succeed
//    } catch {
//      case e: Exception => fail(e)
//    }
//  }
}
