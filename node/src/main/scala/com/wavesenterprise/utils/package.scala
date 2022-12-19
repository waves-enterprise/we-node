package com.wavesenterprise

import cats.Show
import cats.implicits.showInterpolator
import com.google.common.base.Throwables
import monix.execution.UncaughtExceptionReporter
import org.joda.time.Duration
import org.joda.time.format.PeriodFormat

import scala.reflect.runtime.universe
import scala.util.Try

package object utils extends ScorexLogging {

  val UncaughtExceptionsToLogReporter: UncaughtExceptionReporter = UncaughtExceptionReporter(exc => log.error(Throwables.getStackTraceAsString(exc)))

  implicit class PrettyPrintIterable[T](iterable: Iterable[T]) {
    def toPrettyString: String =
      iterable.mkString("[", ", ", "]")
  }

  def forceStopApplication(reason: ApplicationStopReason = Default): Unit =
    new Thread(() => {
                 System.exit(reason.code)
               },
               "we-platform-shutdown-thread").start()

  def humanReadableSize(bytes: Long, si: Boolean = true): String = {
    val (baseValue, unitStrings) =
      if (si)
        (1000, Vector("B", "kB", "MB", "GB", "TB", "PB", "EB", "ZB", "YB"))
      else
        (1024, Vector("B", "KiB", "MiB", "GiB", "TiB", "PiB", "EiB", "ZiB", "YiB"))

    @scala.annotation.tailrec
    def getExponent(curBytes: Long, baseValue: Int, curExponent: Int = 0): Int =
      if (curBytes < baseValue) curExponent
      else {
        val newExponent = 1 + curExponent
        getExponent(curBytes / (baseValue * newExponent), baseValue, newExponent)
      }

    val exponent   = getExponent(bytes, baseValue)
    val divisor    = Math.pow(baseValue, exponent)
    val unitString = unitStrings(exponent)

    f"${bytes / divisor}%.1f $unitString"
  }

  def humanReadableDuration(duration: Long): String = {
    val d = new Duration(duration)
    PeriodFormat.getDefault.print(d.toPeriod)
  }

  def objectFromString[T](fullClassName: String): Try[T] = Try {
    val runtimeMirror = universe.runtimeMirror(getClass.getClassLoader)
    val module        = runtimeMirror.staticModule(fullClassName)
    val obj           = runtimeMirror.reflectModule(module)
    obj.instance.asInstanceOf[T]
  }

  implicit class TrimmedShowIterable[T: Show](xs: Iterable[T]) {
    def mkStringTrimmed(start: String, sep: String, end: String, trim: Int): String = {

      @scala.annotation.tailrec
      def recursive(rest: Iterator[T], displayedString: String = "", displayedLength: Int = 0): String = {
        if (rest.hasNext) {
          if (displayedLength <= trim) {
            val entry = rest.next()
            val updatedOut =
              if (displayedLength == 0)
                show"$entry"
              else
                displayedString + sep + show"$entry"

            recursive(rest, updatedOut, displayedLength + 1)
          } else {
            displayedString + sep + "..."
          }
        } else {
          displayedString
        }
      }

      start + recursive(xs.iterator) + end
    }

    def mkStringTrimmed(trim: Int): String =
      mkStringTrimmed(start = "[", sep = ", ", end = "]", trim)
  }
}
