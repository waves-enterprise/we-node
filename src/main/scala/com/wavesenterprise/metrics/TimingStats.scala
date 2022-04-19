package com.wavesenterprise.metrics

import java.util.concurrent.TimeUnit

import com.wavesenterprise.block.{Block, MicroBlock, TxMicroBlock, VoteMicroBlock}
import com.wavesenterprise.consensus.PoALikeConsensus.RoundTimestamps
import org.influxdb.dto.Point

/**
  * New metrics, used to monitor PoA rounds and key/micro-block timestamps
  */
object TimingStats {
  def roundTimestamps(roundTimestamps: RoundTimestamps): Unit =
    Metrics.writeManyWithoutTime(
      MetricsType.PoA, {
        val startPoint = Point
          .measurement("round_start")
          .time(roundTimestamps.roundStart, TimeUnit.MILLISECONDS)

        val syncPoint = Point
          .measurement("round_sync")
          .time(roundTimestamps.syncStart, TimeUnit.MILLISECONDS)

        val endPoint = Point
          .measurement("round_end")
          .time(roundTimestamps.roundEnd, TimeUnit.MILLISECONDS)

        startPoint :: syncPoint :: endPoint :: Nil
      }
    )

  def keyBlockTime(block: Block): Unit =
    Metrics.writeWithoutTime(MetricsType.PoA, {
      Point
        .measurement("block_timestamp")
        .time(block.timestamp, TimeUnit.MILLISECONDS)
    })

  def microBlockTime(microBlock: MicroBlock): Unit =
    Metrics.writeWithoutTime(
      MetricsType.PoA, {
        Point
          .measurement(microBlock match {
            case _: TxMicroBlock   => "tx_microblock_timestamp"
            case _: VoteMicroBlock => "vote_microblock_timestamp"
          })
          .time(microBlock.timestamp, TimeUnit.MILLISECONDS)
      }
    )
}
