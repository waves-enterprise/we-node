package com.wavesenterprise.network

import java.util.concurrent.TimeUnit

import cats.implicits._
import com.google.common.cache.CacheBuilder
import com.wavesenterprise.network.SyncChannelSelector.{ScoredChannelInfo, Stats, SyncChannelUpdateEvent}
import com.wavesenterprise.network.peers.ActivePeerConnections
import com.wavesenterprise.utils.ScorexLogging
import io.netty.channel._
import monix.eval.{Coeval, Task}
import monix.execution.Scheduler
import monix.reactive.{Observable, OverflowStrategy}

import scala.collection.JavaConverters._
import scala.concurrent.duration.FiniteDuration

/**
  * Produces sync channel update events, observing the score evolution.
  */
class ScoringSyncChannelSelector(
    scoreTtl: FiniteDuration,
    scoreThrottleDuration: FiniteDuration,
    initLocalScore: BigInt,
    lastBlockScoreEvents: Observable[BigInt],
    remoteScoreEvents: ChannelObservable[BigInt],
    channelCloseEvents: Observable[Channel],
    activePeerConnections: ActivePeerConnections
)(implicit scheduler: Scheduler)
    extends ScorexLogging
    with SyncChannelSelector {

  @volatile private[this] var localScore: BigInt                  = initLocalScore
  @volatile private[this] var currentSyncChannel: Option[Channel] = None

  private val scores = CacheBuilder
    .newBuilder()
    .expireAfterWrite(scoreTtl.toMillis, TimeUnit.MILLISECONDS)
    .build[Channel, BigInt]()
    .asMap()
    .asScala

  private def localScoreUpdates: Observable[Unit] = {
    lastBlockScoreEvents
      .asyncBoundary(OverflowStrategy.Default)
      .distinctUntilChanged
      .map { newLocalScore =>
        log.debug(s"New local score: '$newLocalScore', old: '$localScore', Δ'${newLocalScore - localScore}'")
        localScore = newLocalScore
        activePeerConnections.broadcast(LocalScoreChanged(newLocalScore))
      }
  }

  private def remoteScoresUpdates: Observable[Unit] = {
    val scoreByChannel: ChannelObservable[BigInt] = remoteScoreEvents
      .asyncBoundary(OverflowStrategy.Default)
      .groupBy { case (ch, _) => ch }
      .mergeMap(_.distinctUntilChanged.throttleLast(scoreThrottleDuration))

    scoreByChannel.map {
      case (ch, score) =>
        scores.put(ch, score)
        val maybeCurrentBest   = currentSyncChannel.flatMap(scores.get)
        val compareMessagePart = maybeCurrentBest.map(currentBest => s", current best '$currentBest', Δ'${score - currentBest}'").orEmpty
        log.debug(s"New remote score '$score' by channel '${id(ch)}'" + compareMessagePart)
    }
  }

  private def closeChannelsUpdates: Observable[Unit] = {
    channelCloseEvents
      .asyncBoundary(OverflowStrategy.Default)
      .map { ch =>
        scores.remove(ch)
        if (currentSyncChannel.contains(ch)) {
          log.debug(s"Best channel '${id(ch)}' has been closed")
          currentSyncChannel = None
        }
      }
  }

  private def calcBestChannel(currentBestChannel: Option[Channel],
                              currentLocalScore: BigInt,
                              scoreMap: scala.collection.Map[Channel, BigInt]): Option[ScoredChannelInfo] = {

    val (bestScore, bestChannels) = scoreMap.foldLeft(BigInt(0) -> List.empty[Channel]) {
      case (currentMax @ (maxScore, maxScoreChannels), (channel, score)) =>
        if (score > maxScore) score -> List(channel)
        else if (score == maxScore) maxScore -> (channel :: maxScoreChannels)
        else currentMax
    }

    if (bestScore > currentLocalScore && bestChannels.nonEmpty) {
      currentBestChannel match {
        case Some(current) if bestChannels.contains(current) =>
          log.trace(s"Current best channel '${id(current)}' still a leader with score '$bestScore'")
          Some(ScoredChannelInfo(current, bestScore))
        case _ =>
          val head = bestChannels.head
          Some(ScoredChannelInfo(head, bestScore))
      }
    } else {
      None
    }
  }

  override val syncChannelUpdateEvents: Observable[SyncChannelUpdateEvent] =
    Observable(localScoreUpdates, remoteScoresUpdates, closeChannelsUpdates).merge
      .map { _ =>
        val newSyncChannelOpt = calcBestChannel(currentSyncChannel, localScore, scores)
        currentSyncChannel = newSyncChannelOpt.map(_.channel)
        SyncChannelSelector.SyncChannelUpdateEvent(newSyncChannelOpt)
      }
      .distinctUntilChanged
      .doOnNext(event => Task.eval(log.info(s"Updated sync channel '${event.currentChannelOpt}'")))
      .logErr
      .onErrorRestartUnlimited
      .share(scheduler)

  override val statsReporter: Coeval[Stats] = Coeval.eval {
    SyncChannelSelector.ScoringStats(currentSyncChannel.map(id(_)), localScore, scores.size)
  }

}
