package com.wavesenterprise.database.snapshot

import com.wavesenterprise.TestSchedulers.consensualSnapshotScheduler
import com.wavesenterprise.block.{BlockHeader, SignerData}
import com.wavesenterprise.consensus.PoALikeConsensusBlockData
import com.wavesenterprise.database.snapshot.PackedSnapshot.PackedSnapshotFile
import com.wavesenterprise.lagonaki.mocks.TestBlock.defaultSigner
import com.wavesenterprise.network.netty.handler.stream.ChunkedStream
import com.wavesenterprise.network.peers.ActivePeerConnections
import com.wavesenterprise.network.{SnapshotNotification, SnapshotRequest}
import com.wavesenterprise.state.{Blockchain, ByteStr}
import com.wavesenterprise.transaction.{BlockchainUpdater, LastBlockInfo}
import com.wavesenterprise.utils.EitherUtils.EitherExt
import com.wavesenterprise.utils.ResourceUtils
import com.wavesenterprise.{RxSetup, crypto}
import io.netty.buffer.Unpooled
import io.netty.channel.Channel
import monix.eval.Task
import monix.reactive.subjects.ConcurrentSubject
import monix.reactive.{Observable, Observer}
import org.scalamock.scalatest.MockFactory

import java.io.ByteArrayInputStream
import java.nio.file.{Files, Path}
import scala.concurrent.duration._
import scala.util.Random
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers

class SnapshotLoaderSpec extends AnyFreeSpec with PeerConnectionGen with MockFactory with Matchers with RxSetup {

  trait MockedBlockchain extends BlockchainUpdater with Blockchain

  case class FixtureParams(loader: SnapshotLoader,
                           connections: ActivePeerConnections,
                           notifications: Observer[(Channel, SnapshotNotification)],
                           statusObservable: Observable[SnapshotStatus],
                           snapshotDir: Path)

  protected def fixture(test: FixtureParams => Unit): Unit = {
    withDirectory("snapshot-loader-test") { snapshotDir =>
      val settings        = ConsensualSnapshotTest.createSnapshotSettings()
      val notifications   = ConcurrentSubject.publish[(Channel, SnapshotNotification)](consensualSnapshotScheduler)
      val statusPublisher = ConcurrentSubject.publish[SnapshotStatus](consensualSnapshotScheduler)
      val connections     = new ActivePeerConnections

      val blockchain = mock[MockedBlockchain]
      (blockchain.lastBlockInfo _)
        .expects()
        .returning {
          Observable(LastBlockInfo(ByteStr.empty, settings.snapshotSendingHeight, BigInt(1), ready = true))
        }
        .atLeastOnce()
      (blockchain
        .blockHeaderAndSize(_: Int))
        .expects(settings.snapshotHeight.value)
        .returning {
          Some((BlockHeader(1, 2, ByteStr.empty, SignerData(defaultSigner, ByteStr.empty), PoALikeConsensusBlockData(0), Set.empty, None, 1), 1))
        }
        .atLeastOnce()

      try {
        ResourceUtils.withResource {
          SnapshotLoader(
            nodeOwner = crypto.generatePublicKey,
            snapshotDirectory = snapshotDir.toString,
            settings = settings,
            connections = connections,
            blockchain = blockchain,
            notifications = notifications,
            statusObserver = statusPublisher,
            unpackSnapshot = Task(Right(())),
            timeSpans = LoaderTimeSpans(10.seconds, 1.second, 1.second)
          )(consensualSnapshotScheduler)
        } { loader =>
          test(FixtureParams(loader, connections, notifications, statusPublisher, snapshotDir))
        }
      } finally {
        notifications.onComplete()
        statusPublisher.onComplete()
      }
    }
  }

  "should receive snapshot notification" in fixture {
    case FixtureParams(loader, connections, notifications, _, _) =>
      val (connection, _) = createPeerConnection(newChannel())
      connections.putIfAbsent(connection).explicitGet()

      val task = (for {
        size         <- Task(Random.nextInt(Short.MaxValue))
        notification <- Task(SnapshotNotification(defaultSigner, size))
        _            <- send(notifications)((connection.channel, notification))
        _            <- Task.sleep(500.millis)
      } yield {
        loader.snapshotOwners should contain theSameElementsAs Map(notification.sender -> notification)
      }).guarantee(Task(connection.channel.close()))

      test(task, 5.seconds)
  }

  "should successfully load snapshot" in fixture {
    case FixtureParams(_, connections, notifications, statusObservable, snapshotDir) =>
      val channel = newChannel()

      val connection = createPeerConnection(channel, defaultSigner)
      connections.putIfAbsent(connection).explicitGet()

      val statusEvents = loadEvents(statusObservable)

      val task = (for {
        size         <- Task(Random.nextInt(Short.MaxValue))
        notification <- Task(SnapshotNotification(defaultSigner, size))
        _            <- send(notifications)((channel, notification))
        _ <- Task(channel.readOutbound[AnyRef]()).delayExecution(100.millis).restartUntil { msg =>
          Option(msg).exists(_.isInstanceOf[SnapshotRequest])
        }
        snapshotBytes <- Task(randomBytes(Short.MaxValue))
        _ <- Task(new ChunkedStream(new ByteArrayInputStream(snapshotBytes))).bracket { file =>
          Task {
            Iterator.continually(file.readChunk(channel.alloc())).takeWhile(_ != null).foreach { chunk =>
              channel.writeInbound(chunk)
            }
            channel.writeInbound(Unpooled.EMPTY_BUFFER)
          }
        } { stream =>
          Task {
            stream.close()
            channel.finish()
          }
        }
        status <- restartUntilStatus[Exists.type](statusEvents)
      } yield {
        status shouldBe Exists

        val snapshotFile = snapshotDir.resolve(PackedSnapshotFile)
        Files.readAllBytes(snapshotFile) shouldBe snapshotBytes
      }).guarantee(Task(channel.close()))

      test(task, 30.seconds)
  }

}
