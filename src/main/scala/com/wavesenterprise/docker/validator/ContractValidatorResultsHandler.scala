package com.wavesenterprise.docker.validator

import com.wavesenterprise.docker.validator.ContractValidatorMeasurementType._
import com.wavesenterprise.network.peers.ActivePeerConnections
import com.wavesenterprise.network.{ChannelObservable, ContractValidatorResults}
import com.wavesenterprise.utils.ScorexLogging
import com.wavesenterprise.utx.UtxPool
import io.netty.channel.Channel
import io.netty.channel.group.ChannelMatcher
import monix.execution.Scheduler
import monix.reactive.Observable

import scala.concurrent.duration._

class ContractValidatorResultsHandler(activePeerConnections: ActivePeerConnections,
                                      utxPool: UtxPool,
                                      resultsStoreOpt: Option[ContractValidatorResultsStore])(implicit val scheduler: Scheduler)
    extends ScorexLogging {

  import ContractValidatorResultsHandler._

  def subscribe(observable: ChannelObservable[ContractValidatorResults]): Unit = {
    observable
      .observeOn(scheduler)
      .bufferTimedAndCounted(MaxBufferTime, MaxBuffersSize)
      .foreach { messagesBuffer =>
        val toAdd = messagesBuffer.filter {
          case (_, message) =>
            def inUtx: Boolean = utxPool.contains(message.txId) || utxPool.containsInsideAtomic(message.txId)
            message.signatureValid() && inUtx && !resultsStoreOpt.exists(_.contains(message))
        }

        if (toAdd.nonEmpty) {
          toAdd
            .groupBy { case (channel, _) => channel }
            .foreach {
              case (sender, xs) => handleMessages(sender, xs)
            }
          activePeerConnections.flushWrites()
        }
      }

    resultsStoreOpt.foreach { store =>
      Observable
        .interval(MetricsUpdatePeriod)
        .observeOn(scheduler)
        .foreach { _ =>
          val time = System.currentTimeMillis()
          ContractValidatorStoreMetrics.writeRawNumber(BlocksCount, store.blocksSize, time)
          ContractValidatorStoreMetrics.writeRawNumber(TransactionsCount, store.txsSize, time)
          ContractValidatorStoreMetrics.writeRawNumber(ResultsCount, store.size, time)
        }
    }
  }

  private def handleMessages(sender: Channel, messages: Seq[(Channel, ContractValidatorResults)]): Unit = {
    val channelMatcher: ChannelMatcher = { (_: Channel) != sender }
    messages.foreach {
      case (_, message) =>
        resultsStoreOpt.foreach(_.add(message))
        activePeerConnections.writeMsg(message, channelMatcher)
    }
  }
}

object ContractValidatorResultsHandler {

  private val MaxBufferTime       = 200.millis
  private val MaxBuffersSize      = 10
  private val MetricsUpdatePeriod = 5.seconds
}
