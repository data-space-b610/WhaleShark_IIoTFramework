package ksb.csle.component.ingestion.util

import scala.collection._

import ksb.csle.common.annotation.Experimental
import org.apache.spark.rdd.RDD

object BufferUtils {

  @Experimental
  private[component] class WindowedBuffer[T](windowSize: Int,
      slideSize: Int) extends Serializable {

    if (windowSize <= 0)
      throw new IllegalArgumentException("'windowSize' must be positive")
    if (slideSize <= 0)
      throw new IllegalArgumentException("'slideSize' must be positive")

    private[this] val buf = new mutable.ArrayBuffer[T](windowSize + slideSize)

    def size(): Int = synchronized {
      buf.length
    }

    def add(elem: T): Unit = synchronized {
      buf.append(elem)
    }

    def addAll(xs: TraversableOnce[T]): Unit = synchronized {
      buf.appendAll(xs)
    }

    def get(): Seq[T] = synchronized {
      getUnsafely()
    }

    private def getUnsafely(): Seq[T] = {
      if (buf.size < windowSize) {
        Seq.empty
      } else {
        val c = buf.take(windowSize).toSeq
        buf.remove(0, slideSize)
        c
      }
    }

    def getAll(): Seq[Seq[T]] = synchronized {
      var loop = true
      val list = new mutable.ListBuffer[Seq[T]]()
      while (loop) {
        val windowed = getUnsafely()
        if (windowed.isEmpty) {
          loop = false
        } else {
          list.append(windowed)
        }
      }
      list.toSeq
    }

    override def toString(): String = {
      s"(${buf.size}, $buf)"
    }
  }
}
