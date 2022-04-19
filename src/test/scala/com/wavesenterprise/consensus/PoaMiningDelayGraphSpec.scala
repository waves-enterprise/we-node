package com.wavesenterprise.consensus

import com.panayotis.gnuplot.JavaPlot
import com.panayotis.gnuplot.plot.DataSetPlot
import com.panayotis.gnuplot.style.{PlotStyle, Style}
import com.wavesenterprise.{NoShrink, TransactionGen}
import org.scalatest.{DoNotDiscover, FreeSpec, Matchers}
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.duration._

@DoNotDiscover
class PoaMiningDelayGraphSpec extends FreeSpec with Matchers with ScalaCheckPropertyChecks with TransactionGen with NoShrink {
  private val roundAndSyncDuration = 10 seconds

  "miningDelay graph" in {

    val p = new JavaPlot()

    p.setTitle("Mining delay sleep distribution")
    p.getAxis("x").setLabel("time", "Arial", 20)
    p.getAxis("y").setLabel("mining delay", "Arial", 20)
    p.setKey(JavaPlot.Key.TOP_RIGHT)

    val startTime                            = 200000L
    val endTime                              = startTime + roundAndSyncDuration.toMillis
    val dataPoints                           = 1000
    val plotData: ArrayBuffer[Array[Double]] = new ArrayBuffer[Array[Double]](dataPoints)

    val step: Long = (endTime - startTime) / dataPoints

    for (i <- 0 until dataPoints) {
      val currentTime = startTime + step * i
      val miningDelay = PoALikeConsensus.calculatePeriodicDelay(currentTime, endTime, roundAndSyncDuration.toMillis, maxDelay = 1.second)
      val dataPoint   = List(currentTime.toDouble, miningDelay.toMillis.toDouble).toArray
      plotData.insert(i, dataPoint)
    }

    val myPlotStyle = new PlotStyle
    myPlotStyle.setStyle(Style.POINTS)
    myPlotStyle.setLineWidth(1)
    myPlotStyle.setLineType(1)

    val dataSet = new DataSetPlot(plotData.toArray)
    dataSet.setPlotStyle(myPlotStyle)
    p.addPlot(dataSet)

    val boundSet = new DataSetPlot(Array(Array(startTime, 0D), Array(endTime, 0D)))
    p.addPlot(boundSet)

    p.plot()
  }

}
