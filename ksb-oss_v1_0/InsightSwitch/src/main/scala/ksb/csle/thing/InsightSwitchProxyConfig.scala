package ksb.csle.thing

import java.io.FileInputStream
import java.net.URI
import java.util.ArrayList
import java.util.List
import java.util.Properties
import scala.beans.{ BeanProperty, BooleanBeanProperty }
import scala.collection.JavaConversions._

class InsightSwitchProxyConfig {

  @BeanProperty
  var sensingPeriod: Long = _

  @BeanProperty
  var friendlyNamePrefixList: List[String] = new ArrayList[String]()

  def load(filePath: String): Unit = {
    var in: FileInputStream = null
    val props: Properties = new Properties()
    try {
      in = new FileInputStream(filePath)
      props.loadFromXML(in)
    } catch {
      case e: Exception => {
        LOG.error(e.getMessage, e)
      }

    } finally if (in != null) {
      try in.close()
      catch {
        case e: Exception => LOG.error(e.getMessage, e)

      }
    }
    setSensingPeriod(props)
    setFriendlyNamePrefixList(props)
  }

  private def setSensingPeriod(props: Properties): Unit = {
    val value: String = props.getProperty("pc.pc.sensingPeriod")
    if (value == null) {
      this.sensingPeriod = DEFAULT_SENSING_PERIOD
    } else {
      try this.sensingPeriod = java.lang.Long.valueOf(value)
      catch {
        case e: Exception => {
          LOG.error(e.getMessage, e)
          this.sensingPeriod = DEFAULT_SENSING_PERIOD
        }

      }
    }
  }

  private def setFriendlyNamePrefixList(props: Properties): Unit = {
    val value: String = props.getProperty("pc.friendlyNamePrefixList")
    if (value != null) {
      val array: Array[String] = value.split(",")
      for (i <- 0 until array.length) {
        this.friendlyNamePrefixList.add(array(i))
      }
    }
  }

}
