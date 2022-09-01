package com.wavesenterprise.metrics

import org.influxdb.dto.Point

object TxsInBlockchainStats {
  def record(number: Int): Unit =
    Metrics.write(MetricsType.Common,
                  Point
                    .measurement("applied-txs")
                    .addField("n", number))
}
