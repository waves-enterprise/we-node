package com.wavesenterprise.network

import com.wavesenterprise.network.SyncChannelSelector.{ChannelInfo, ScoredChannelInfo}
import com.wavesenterprise.network.peers.ActivePeerConnections
import com.wavesenterprise.{RxSetup, TransactionGen}
import io.netty.channel.Channel
import io.netty.channel.group.ChannelGroupFuture
import io.netty.channel.local.LocalChannel
import monix.eval.Task
import monix.execution.atomic.AtomicInt
import monix.reactive.subjects.PublishSubject
import org.scalamock.scalatest.MockFactory
import org.scalatest.{Assertion, FreeSpec, Matchers}

import scala.concurrent.duration._

class ScoringSyncChannelSelectorSpec extends FreeSpec with Matchers with TransactionGen with RxSetup with MockFactory {

  case class FixtureParams(localScoreEvents: PublishSubject[BigInt],
                           remoteScoreEvents: PublishSubject[(Channel, BigInt)],
                           channelCloseEvents: PublishSubject[Channel],
                           activePeerConnections: ActivePeerConnections,
                           eventLoader: Task[Seq[Option[ChannelInfo]]])

  def fixture(activePeerConnectionsMock: Boolean = true)(testBlock: FixtureParams => Task[Assertion]): Assertion = {
    val localScoreEvents   = PublishSubject[BigInt]
    val remoteScoreEvents  = PublishSubject[(Channel, BigInt)]
    val channelCloseEvents = PublishSubject[Channel]

    val activePeerConnections = mock[ActivePeerConnections]
    if (activePeerConnectionsMock) {
      (activePeerConnections.broadcast(_: Any, _: Set[Channel])).expects(*, *).anyNumberOfTimes()
    }

    val syncChannelSelector = new ScoringSyncChannelSelector(
      scoreTtl = 1.minute,
      scoreThrottleDuration = 0.seconds,
      initLocalScore = 0,
      lastBlockScoreEvents = localScoreEvents,
      remoteScoreEvents = remoteScoreEvents,
      channelCloseEvents = channelCloseEvents,
      activePeerConnections = activePeerConnections
    )

    val eventLoader: Task[Seq[Option[ChannelInfo]]] = {
      loadEvents(syncChannelSelector.syncChannelUpdateEvents.map(_.currentChannelOpt))
    }

    val params = FixtureParams(localScoreEvents, remoteScoreEvents, channelCloseEvents, activePeerConnections, eventLoader)

    try {
      test(testBlock(params))
    } finally {
      localScoreEvents.onComplete()
      remoteScoreEvents.onComplete()
      channelCloseEvents.onComplete()
    }
  }

  "should broadcast local score" in fixture(activePeerConnectionsMock = false) {
    case FixtureParams(localScoreEvents, _, _, activePeerConnections, loadEvents) =>
      val writeCount = AtomicInt(0)

      (activePeerConnections
        .broadcast(_: Any, _: Set[Channel]))
        .expects(*, *)
        .onCall((_: Any, _: Set[Channel]) => {
          writeCount.incrementAndGet(); mock[ChannelGroupFuture]
        })
        .anyNumberOfTimes()

      for {
        _ <- send(localScoreEvents)(1)
        _ <- loadEvents
        _ = writeCount.get shouldBe 1
        _ <- send(localScoreEvents)(2)
        _ <- loadEvents
      } yield {
        writeCount.get shouldBe 2
      }
  }

  "should emit better channel" - {
    "when a new channel has the better score than the local one" in fixture() {
      case FixtureParams(localScoreEvents, remoteScoreEvents, _, _, loadEvents) =>
        val testChannel = new LocalChannel()

        for {
          _          <- send(localScoreEvents)(1)
          newScores1 <- loadEvents
          _          <- send(remoteScoreEvents)((testChannel, 2))
          newScores2 <- loadEvents
        } yield {
          newScores1 shouldBe List(None)
          newScores2 shouldBe List(Some(ScoredChannelInfo(testChannel, 2)))
        }
    }

    "when the connection with the best one is closed" in fixture() {
      case FixtureParams(localScoreEvents, remoteScoreEvents, channelCloseEvents, _, loadEvents) =>
        val ch100 = new LocalChannel()
        val ch200 = new LocalChannel()

        for {
          _          <- send(localScoreEvents)(1)
          _          <- send(remoteScoreEvents)((ch200, 200))
          newScores1 <- loadEvents
          _          <- send(remoteScoreEvents)((ch100, 100))
          _          <- send(channelCloseEvents)(ch200)
          newScores2 <- loadEvents
          _          <- send(channelCloseEvents)(ch100)
          newScores3 <- loadEvents
        } yield {
          newScores1.last shouldBe Some(ScoredChannelInfo(ch200, 200))
          newScores2.last shouldBe Some(ScoredChannelInfo(ch100, 100))
          newScores3.last shouldBe None
        }
    }

    "when the best channel upgrades score" in fixture() {
      case FixtureParams(localScoreEvents, remoteScoreEvents, _, _, loadEvents) =>
        val testChannel = new LocalChannel()

        for {
          _          <- send(localScoreEvents)(1)
          newScores1 <- loadEvents
          _          <- send(remoteScoreEvents)((testChannel, 2))
          newScores2 <- loadEvents
          _          <- send(remoteScoreEvents)((testChannel, 3))
          newScores3 <- loadEvents
        } yield {
          newScores1 shouldBe List(None)
          newScores2 shouldBe List(Some(ScoredChannelInfo(testChannel, 2)))
          newScores3 shouldBe List(Some(ScoredChannelInfo(testChannel, 3)))
        }
    }

    "when the best channel downgrades score" in fixture() {
      case FixtureParams(localScoreEvents, remoteScoreEvents, _, _, loadEvents) =>
        val testChannel = new LocalChannel()

        for {
          _          <- send(localScoreEvents)(1)
          newScores1 <- loadEvents
          _          <- send(remoteScoreEvents)((testChannel, 3))
          newScores2 <- loadEvents
          _          <- send(remoteScoreEvents)((testChannel, 2))
          newScores3 <- loadEvents
        } yield {
          newScores1 shouldBe List(None)
          newScores2 shouldBe List(Some(ScoredChannelInfo(testChannel, 3)))
          newScores3 shouldBe List(Some(ScoredChannelInfo(testChannel, 2)))
        }
    }
  }

  "should emit None" - {
    "stop when local score is as good as network's" in fixture() {
      case FixtureParams(localScoreEvents, remoteScoreEvents, _, _, loadEvents) =>
        val ch100 = new LocalChannel()

        for {
          _         <- send(localScoreEvents)(1)
          _         <- send(remoteScoreEvents)((ch100, 100))
          _         <- loadEvents
          _         <- send(localScoreEvents)(100)
          newScores <- loadEvents
        } yield {
          newScores.last shouldBe None
        }
    }

    "stop when local score is better than network's" in fixture() {
      case FixtureParams(localScoreEvents, remoteScoreEvents, _, _, loadEvents) =>
        val ch100 = new LocalChannel()

        for {
          _         <- send(localScoreEvents)(1)
          _         <- send(remoteScoreEvents)((ch100, 100))
          _         <- loadEvents
          _         <- send(localScoreEvents)(101)
          newScores <- loadEvents
        } yield {
          newScores.last shouldBe None
        }
    }
  }

  "should not emit anything" - {
    "when current best channel is not changed and its score is not changed" - {

      "directly" in fixture() {
        case FixtureParams(localScoreEvents, remoteScoreEvents, _, _, loadEvents) =>
          val ch100 = new LocalChannel()

          for {
            _         <- send(localScoreEvents)(1)
            _         <- send(remoteScoreEvents)((ch100, 100))
            _         <- loadEvents
            _         <- send(remoteScoreEvents)((ch100, 100))
            newScores <- loadEvents
          } yield {
            newScores shouldBe 'empty
          }
      }

      "indirectly" in fixture() {
        case FixtureParams(localScoreEvents, remoteScoreEvents, _, _, loadEvents) =>
          val ch100 = new LocalChannel()

          for {
            _         <- send(localScoreEvents)(1)
            _         <- send(remoteScoreEvents)((ch100, 100))
            _         <- loadEvents
            _         <- send(localScoreEvents)(2)
            newScores <- loadEvents
          } yield {
            newScores shouldBe 'empty
          }
      }
    }
  }
}
