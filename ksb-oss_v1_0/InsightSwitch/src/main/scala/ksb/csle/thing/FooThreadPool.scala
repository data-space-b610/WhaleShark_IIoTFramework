package ksb.csle.thing

import java.util.concurrent.ArrayBlockingQueue
import java.util.concurrent.ThreadPoolExecutor
import java.util.concurrent.TimeUnit
import FooThreadPool._
import scala.collection.JavaConversions._

object FooThreadPool {
}

class FooThreadPool(corePoolSize: Int, maxPoolSize: Int, keepaliveTime: Long) {

  private var pool: ThreadPoolExecutor = new ThreadPoolExecutor(
    corePoolSize,
    maxPoolSize,
    keepaliveTime,
    TimeUnit.SECONDS,
    new ArrayBlockingQueue[Runnable](DEFAULT_POOL_SIZE))

  def this() =
    this(DEFAULT_POOL_SIZE, DEFAULT_POOL_SIZE, DEFAULT_KEEPALIVE_TIME)

  def this(poolSize: Int) = this(poolSize, poolSize, DEFAULT_KEEPALIVE_TIME)

  def remainingCapacity(): Int = this.pool.getQueue.remainingCapacity()

  def runTask(task: Runnable): Unit = {
    this.pool.execute(task)
  }

  def shutDown(): Unit = {
    this.pool.shutdown()
  }

  def isShutDown(): Boolean = this.pool.isShutdown

  override def toString(): String = {
    val sb: StringBuffer = new StringBuffer()
    sb.append("corePoolSize = ")
    sb.append(this.pool.getCorePoolSize)
    sb.append(", activeCount = ")
    sb.append(this.pool.getActiveCount)
    sb.append(", queueSize = ")
    sb.append(this.pool.getQueue.size)
    sb.append(", remainingCapacity = ")
    sb.append(this.pool.getQueue.remainingCapacity())
    sb.append(", taskCount = ")
    sb.append(this.pool.getTaskCount)
    sb.toString
  }

}
