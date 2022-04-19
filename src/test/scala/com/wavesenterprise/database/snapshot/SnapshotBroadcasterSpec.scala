package com.wavesenterprise.database.snapshot

import com.wavesenterprise.RxSetup
import com.wavesenterprise.TestSchedulers.consensualSnapshotScheduler
import com.wavesenterprise.database.snapshot.SnapshotBroadcaster.RequestsParallelism
import com.wavesenterprise.lagonaki.mocks.TestBlock.defaultSigner
import com.wavesenterprise.network.netty.handler.stream.StreamHandlerBase
import com.wavesenterprise.network.peers.ActivePeerConnections
import com.wavesenterprise.network.{SnapshotNotification, SnapshotRequest}
import com.wavesenterprise.state._
import com.wavesenterprise.transaction.LastBlockInfo
import com.wavesenterprise.utils.EitherUtils.EitherExt
import com.wavesenterprise.utils.ResourceUtils
import io.netty.buffer.{ByteBuf, Unpooled}
import io.netty.channel.embedded.EmbeddedChannel
import io.netty.channel.{Channel, DefaultChannelId}
import monix.eval.Task
import monix.execution.Ack
import monix.reactive.subjects.ConcurrentSubject
import monix.reactive.{Observable, Observer}
import org.scalamock.scalatest.MockFactory
import org.scalatest.{FreeSpec, Matchers}

import java.nio.file.Files
import scala.concurrent.duration._

class SnapshotBroadcasterSpec extends FreeSpec with MockFactory with Matchers with PeerConnectionGen with RxSetup {

  case class FixtureParams(broadcaster: SnapshotBroadcaster,
                           connections: ActivePeerConnections,
                           requests: Observer[(Channel, SnapshotRequest)],
                           size: Long)

  protected def fixture(withWriteThrottling: Boolean = false)(test: FixtureParams => Unit): Unit = {
    withDirectory("snapshot-broadcaster-test") { snapshotDir =>
      val settings    = ConsensualSnapshotTest.createSnapshotSettings()
      val requests    = ConcurrentSubject.publish[(Channel, SnapshotRequest)](consensualSnapshotScheduler)
      val connections = new ActivePeerConnections

      val blockObservable = Observable(LastBlockInfo(ByteStr.empty, settings.snapshotSendingHeight, BigInt(1), ready = true))

      val snapshotFile = Files.createFile(snapshotDir.resolve(PackedSnapshot.PackedSnapshotFile))
      val bytes        = randomBytes(Short.MaxValue)
      Files.write(snapshotFile, bytes)

      try {
        ResourceUtils.withResource {
          new SnapshotBroadcaster(defaultSigner,
                                  settings,
                                  Observable(Verified),
                                  connections,
                                  blockObservable,
                                  requests,
                                  Task.pure(snapshotFile),
                                  BroadcasterTimeSpans(1.second))(consensualSnapshotScheduler) {
            override protected def writeToChannel(channel: Channel, request: SnapshotRequest): Task[Ack] = {
              if (withWriteThrottling) {
                super.writeToChannel(channel, request).delayExecution(3.seconds)
              } else {
                super.writeToChannel(channel, request)
              }

            }
          }
        } { loader =>
          test(FixtureParams(loader, connections, requests, bytes.length))
        }
      } finally {
        requests.onComplete()
      }
    }
  }

  "should broadcast snapshot notifications to all peers" in fixture() {
    case FixtureParams(broadcaster, connections, _, size) =>
      (1 to 10)
        .map { _ =>
          createPeerConnection(newChannel())
        }
        .foreach {
          case (connection, _) =>
            connections.putIfAbsent(connection).explicitGet()
        }

      Thread.sleep(2 * broadcaster.timeSpans.broadcastInterval.toMillis)

      val notification = SnapshotNotification(defaultSigner, size)
      connections.connections.foreach { connection =>
        connection.channel.asInstanceOf[EmbeddedChannel].readOutbound[SnapshotNotification]() shouldBe notification
      }
  }

  "should process request" in fixture() {
    case FixtureParams(broadcaster, connections, requests, size) =>
      val channel              = newChannel()
      val (connection, sender) = createPeerConnection(channel)
      connections.putIfAbsent(connection)

      val request           = SnapshotRequest(sender, 0)
      val completedRequests = loadEvents(broadcaster.completedRequests())

      val task = for {
        _         <- send(requests)((channel, request))
        completed <- completedRequests
      } yield {
        completed.iterator.next() shouldBe request

        val chunkCount = math.ceil(size.toDouble / StreamHandlerBase.DefaultChunkSize).toInt

        (1 to chunkCount).foreach(_ => channel.readOutbound[ByteBuf]())
        channel.readOutbound[ByteBuf]() shouldBe Unpooled.EMPTY_BUFFER
      }

      test(task, 5.seconds)
  }

  "should drop request while busy" in fixture(withWriteThrottling = true) {
    case FixtureParams(broadcaster, connections, requests, _) =>
      val peers = (1 to 10).map { _ =>
        val channel              = new EmbeddedChannel(DefaultChannelId.newInstance())
        val (connection, sender) = createPeerConnection(channel)
        connections.putIfAbsent(connection)
        (connection, sender)
      }

      val completedRequests = loadEvents(broadcaster.completedRequests(), 6.seconds)

      val task = for {
        _ <- Task.parSequenceUnordered {
          peers.map {
            case (connection, sender) => send(requests)((connection.channel, SnapshotRequest(sender, 0)))
          }
        }
        completed <- completedRequests
      } yield {
        completed.size shouldBe RequestsParallelism
      }

      test(task, 10.seconds)
  }
}
