package com.wavesenterprise.consensus

import java.util.concurrent.{ConcurrentHashMap, TimeUnit}

import com.google.common.cache.CacheBuilder
import com.wavesenterprise.network.peers.ActivePeerConnections
import com.wavesenterprise.network.{ChannelObservable, VoteMessage}
import com.wavesenterprise.settings.ConsensusSettings.CftSettings
import com.wavesenterprise.state.ByteStr
import io.netty.channel.Channel
import io.netty.channel.group.ChannelMatcher
import monix.eval.Task
import monix.execution.Scheduler
import monix.reactive.subjects.ReplaySubject
import monix.reactive.{Observable, OverflowStrategy}

import scala.concurrent.duration._

class BlockVotesHandler(
    cftSettings: CftSettings,
    activePeerConnections: ActivePeerConnections,
    voteEvents: ChannelObservable[Vote]
)(implicit val scheduler: Scheduler)
    extends AutoCloseable {
  import BlockVotesHandler._

  private[this] val knownVotes = CacheBuilder
    .newBuilder()
    .maximumSize(MaxCacheSize)
    .expireAfterWrite(cftSettings.syncDuration.toMillis, TimeUnit.MILLISECONDS)
    .build[ByteStr, Object]

  private[this] val votesMap = new ConcurrentHashMap[ByteStr, ReplaySubject[Vote]]

  def broadcastVote(vote: Vote): Unit = activePeerConnections.broadcast(VoteMessage(vote))

  @inline
  def blockVotes(blockVoteHash: ByteStr): Observable[Vote] = getOrCreate(blockVoteHash)

  def clear(): Unit = votesMap.clear()

  private def getOrCreate(blockVoteHash: ByteStr): ReplaySubject[Vote] = {
    votesMap.computeIfAbsent(blockVoteHash, _ => ReplaySubject[Vote]())
  }

  private val incomingVotesHandling = voteEvents
    .asyncBoundary(OverflowStrategy.Default)
    .bufferTimedAndCounted(MaxBufferTime, MaxBufferSize)
    .mapEval { votes =>
      Task {
        val toAdd = votes.filter {
          case (_, vote) =>
            isUnknown(vote.id()) && vote.signatureValid()
        }

        if (toAdd.nonEmpty) {
          toAdd
            .groupBy { case (channel, _) => channel }
            .foreach {
              case (sender, votesGroup) =>
                handleChannelVotes(sender, votesGroup)
            }
          activePeerConnections.flushWrites()
        }
      }
    }
    .subscribe()

  private def handleChannelVotes(sender: Channel, messages: Seq[(Channel, Vote)]): Unit = {
    val channelMatcher: ChannelMatcher = { (_: Channel) != sender }
    messages.foreach {
      case (_, vote) =>
        getOrCreate(vote.blockVotingHash).onNext(vote)
        activePeerConnections.writeMsg(VoteMessage(vote), channelMatcher)
    }
  }

  override def close(): Unit = incomingVotesHandling.cancel()

  private[this] val dummy = new Object()

  @inline
  private def isUnknown(id: ByteStr): Boolean = {
    var isUnknown = false
    knownVotes.get(id, { () =>
      isUnknown = true
      dummy
    })
    isUnknown
  }
}

object BlockVotesHandler {
  private val MaxBufferTime = 100.millis
  private val MaxBufferSize = 10
  private val MaxCacheSize  = 300
}
