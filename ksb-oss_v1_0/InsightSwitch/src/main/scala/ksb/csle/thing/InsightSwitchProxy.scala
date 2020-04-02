package ksb.csle.thing

import java.net.InetAddress
import java.util.Iterator
import java.util.concurrent.ConcurrentHashMap
import org.apache.http.HttpEntity
import org.apache.http.HttpResponse
import org.apache.http.client.methods.HttpGet
import org.apache.http.impl.client.CloseableHttpClient
import org.apache.http.impl.client.HttpClients
import org.apache.http.util.EntityUtils
import org.apache.log4j.xml.DOMConfigurator
import com.palominolabs.wemo.InsightSwitch
import com.palominolabs.wemo.InsightSwitchListener
import com.palominolabs.wemo.InsightSwitchSeeker
import InsightSwitchProxy._
import scala.collection.JavaConversions._

object InsightSwitchProxy {

  def main(args: Array[String]): Unit = {
    val proxy: InsightSwitchProxy = new InsightSwitchProxy()
    try {
      proxy.init()
      proxy.run()
    } catch {
      case e: Exception => LOG.error(e.getMessage, e)
    }
    System.exit(0)
  }
  
}

class InsightSwitchProxy extends InsightSwitchListener {

  private var homePath: String = System.getProperty("wp.proxy.home")
  private var switchMap: ConcurrentHashMap[String, InsightSwitchWorker] =
    new ConcurrentHashMap[String, InsightSwitchWorker]()
  private var threadPool: FooThreadPool = new FooThreadPool()
  var config: InsightSwitchProxyConfig =
    new InsightSwitchProxyConfig()
  private var seeker: InsightSwitchSeeker = _
  private var on: Boolean = true
  private var STARTED: Boolean = false

  if (this.homePath == null) {
    this.homePath = HOME_PATH
  }

  println("HOME = [" + this.homePath + "]")
  System.setProperty("wp.proxy.home", this.homePath)
  println("http.bind.addr = [" + System.getProperty("http.bind.addr") + "]")
  println("http.bind.port = [" + System.getProperty("http.bind.port") + "]")

  val logConfigFilePath: String = this.homePath + LOG_CONF_FILE_PATH

  try DOMConfigurator.configure(logConfigFilePath)
  catch {
    case e: Exception => LOG.error(e.getMessage, e)
  }

  def init(): Unit = {
    LOG.info("load config")
    val configFilePath: String = this.homePath + CONF_FILE_PATH
    this.config.load(configFilePath)
    val binds: Array[InetAddress] = null
    this.seeker = new InsightSwitchSeeker(
      this,
      this.config.getFriendlyNamePrefixList,
      binds)
    this.seeker.start()
    this.STARTED = true
  }

  def stop(): Unit = {
    LOG.info("stop InsightSwitchProxy")
    this.seeker.stop()
    for (key <- this.switchMap.keySet) {
      try this.switchMap.get(key).close()
      catch {
        case e: Exception => LOG.error(e.getMessage, e)
      }
    }
    this.switchMap.clear()
    this.threadPool.shutDown()
    this.STARTED = false
  }

  private def sleep(): Unit = {
    try Thread.sleep(RETRY_INTERVAL)
    catch {
      case e: InterruptedException => LOG.error(e.getMessage, e)
    }
  }

  def run(): Unit = {
    while (this.STARTED) {
      sleep()
      listConnectedDevices()
    }
  }

  private def setSwitches(on: Boolean): Unit = {
    val it: Iterator[InsightSwitchWorker] = this.switchMap.values.iterator()
    while (it.hasNext) {
      val w: InsightSwitchWorker = it.next()
      try {
        println("[" + w.toString + "] " + on)
        if (on) {
          w.setOn()
        } else {
          w.setOff()
        }
      } catch {
        case e: Exception => LOG.error(e.getMessage, e)
      }
    }
  }

  private def listConnectedDevices(): Unit = {
    
    // TODO modify
    
    println("-------------------------------------------------")
    val ctx: String = queryContext()
    if (ctx != null) {
      if (ctx.contains("HotContext")) {
        println("HotContext")
        setSwitches(true)
      } else {
        println("NOT HotContext")
        setSwitches(false)
      }
    }
    println("-------------------------------------------------")
  }

  def getInsightSwitchIterator(): Iterator[InsightSwitchWorker] =
    this.switchMap.values.iterator()

  override def switchAdded(insightSwitch: InsightSwitch): Unit = {
    LOG.debug("switchAdded; name = {}", insightSwitch.getFriendlyName)
    try this.threadPool.runTask(new InsightSwitchWorker(insightSwitch, this))
    catch {
      case e: Exception => LOG.error(e.getMessage, e)
    }
  }

  override def switchRemoved(friendlyName: String): Unit = {
    LOG.debug("switchRemoved; name = {}", friendlyName)
    try this.switchMap.get(friendlyName).close()
    catch {
      case e: Exception => LOG.error(e.getMessage, e)
    }
  }

  def addDevice(friendlyName: String,
                isw: InsightSwitchWorker): Unit = {
    this.switchMap.put(friendlyName, isw)
    println(
      "switchAdded; name = " + friendlyName + ", size = " +
        this.switchMap.size)
  }

  def removeDevice(friendlyName: String): Unit = {
    this.switchMap.remove(friendlyName)
    println(
      "switchRemoved; name = " + friendlyName + ", size = " +
        this.switchMap.size)
  }

  private def queryContext(): String = {
    val httpGet: HttpGet = new HttpGet(QUERY_URI)
    val client: CloseableHttpClient = HttpClients.createDefault()
    var content: String = null
    try {
      val response: HttpResponse = client.execute(httpGet)
      if (response == null) {
        null
      } else {
        val statusCode: Int = response.getStatusLine.getStatusCode
        val entity: HttpEntity = response.getEntity
        if (entity != null) {
          val body: String = EntityUtils.toString(entity)
          if (body != null) {
            content = body
            
            // TODO parse control info.
            
          }
        }
      }
    } catch {
      case e: Exception => LOG.error(e.getMessage, e)
    } finally try client.close()
    catch {
      case e: Exception => LOG.error(e.getMessage, e)
    }
    content
  }

}
