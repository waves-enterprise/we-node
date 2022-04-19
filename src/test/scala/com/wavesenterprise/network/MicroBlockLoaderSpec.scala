package com.wavesenterprise.network

import com.wavesenterprise.account.PrivateKeyAccount
import com.wavesenterprise.block.{Block, MicroBlock, TxMicroBlock}
import com.wavesenterprise.lagonaki.mocks.TestBlock
import com.wavesenterprise.network.MicroBlockLoader.ReceivedMicroBlock
import com.wavesenterprise.network.peers.ActivePeerConnections
import com.wavesenterprise.settings.PositiveInt
import com.wavesenterprise.settings.SynchronizationSettings.MicroblockSynchronizerSettings
import com.wavesenterprise.state.{ByteStr, _}
import com.wavesenterprise.transaction.transfer.TransferTransactionV2
import com.wavesenterprise.utils.EitherUtils.EitherExt
import com.wavesenterprise.{BlockGen, RxSetup, TestSchedulers, TransactionGen}
import io.netty.channel.Channel
import io.netty.channel.embedded.EmbeddedChannel
import monix.eval.Task
import monix.reactive.subjects.PublishSubject
import org.scalamock.scalatest.MockFactory
import org.scalatest._

import scala.concurrent.duration._

class MicroBlockLoaderSpec extends FreeSpec with Matchers with TransactionGen with RxSetup with BlockGen with MockFactory {

  case class FixtureParams(mbInventoryEvents: PublishSubject[(Channel, MicroBlockInventory)],
                           mbResponseEvents: PublishSubject[(Channel, MicroBlockResponse)],
                           eventLoader: Task[Seq[ReceivedMicroBlock]],
                           ngBaseBlock: Block)

  val settings = MicroblockSynchronizerSettings(1.seconds, 1.minute, 1.minute, 100, PositiveInt(3), PositiveInt(3))

  def fixture(testBlock: FixtureParams => Task[Assertion]): Assertion = {
    val mbInventoryEvents = PublishSubject[(Channel, MicroBlockInventory)]
    val mbResponseEvents  = PublishSubject[(Channel, MicroBlockResponse)]

    val storage                = new MicroBlockLoaderStorage(settings)
    val activePeersConnections = new ActivePeerConnections()
    val ng                     = mock[NG]
    val baseBlock              = TestBlock.create(Seq.empty)
    (ng.currentBaseBlock _).expects().returns(Some(baseBlock)).anyNumberOfTimes()

    val synchronizer = new MicroBlockLoader(
      ng = ng,
      settings = settings,
      activePeerConnections = activePeersConnections,
      incomingMicroBlockInventoryEvents = mbInventoryEvents,
      incomingMicroBlockEvents = mbResponseEvents,
      signatureValidator = new SignatureValidator()(TestSchedulers.signaturesValidationScheduler),
      storage = storage
    )(TestSchedulers.microBlockLoaderScheduler)

    val eventLoader: Task[Seq[ReceivedMicroBlock]] = {
      loadEvents(synchronizer.loadingUpdates)
    }

    val params = FixtureParams(mbInventoryEvents, mbResponseEvents, eventLoader, baseBlock)

    try {
      test(testBlock(params))
    } finally {
      mbInventoryEvents.onComplete()
      mbResponseEvents.onComplete()
    }
  }

  val signer: PrivateKeyAccount = TestBlock.defaultSigner

  def buildMicroBlock(sign: ByteStr, prevSign: ByteStr): MicroBlock = {
    val tx = TransferTransactionV2.selfSigned(signer, None, None, 1, 1, 1, signer.toAddress, Array.emptyByteArray).explicitGet()
    TxMicroBlock.buildAndSign(signer, 1L, Seq(tx), prevSign, sign).explicitGet()
  }

  "should request and propagate next microblock with inventory v1" in fixture {
    case FixtureParams(mbInventoryEvents, mbResponseEvents, loadEvents, _) =>
      val ch = new EmbeddedChannel()

      val firstSign  = buildTestSign(1)
      val secondSign = buildTestSign(2)

      for {
        _ <- send(mbInventoryEvents)(ch -> MicroBlockInventoryV1(signer, secondSign, firstSign))
        _ = ch.readOutbound[MicroBlockRequest] shouldBe MicroBlockRequest(secondSign)
        _              <- send(mbResponseEvents)(ch -> MicroBlockResponse(buildMicroBlock(secondSign, firstSign)))
        newMicroBlocks <- loadEvents
      } yield {
        newMicroBlocks.size shouldBe 1
      }
  }

  "should request and propagate next microblock with inventory v2" in fixture {
    case FixtureParams(mbInventoryEvents, mbResponseEvents, loadEvents, baseBlock) =>
      val ch = new EmbeddedChannel()

      val firstSign  = buildTestSign(1)
      val secondSign = buildTestSign(2)

      for {
        _ <- send(mbInventoryEvents)(ch -> MicroBlockInventoryV2(baseBlock.uniqueId, signer, secondSign, firstSign))
        _ = ch.readOutbound[MicroBlockRequest] shouldBe MicroBlockRequest(secondSign)
        _              <- send(mbResponseEvents)(ch -> MicroBlockResponse(buildMicroBlock(secondSign, firstSign)))
        newMicroBlocks <- loadEvents
      } yield {
        newMicroBlocks.size shouldBe 1
      }
  }

  "should not request and propagate next microblock with inventory v2 if the base block signature does not match the inventory" in fixture {
    case FixtureParams(mbInventoryEvents, _, _, _) =>
      val ch = new EmbeddedChannel()

      val firstSign  = buildTestSign(1)
      val secondSign = buildTestSign(2)

      for {
        _ <- send(mbInventoryEvents)(ch -> MicroBlockInventoryV2(buildTestSign(3), signer, secondSign, firstSign))
      } yield {
        ch.readOutbound[MicroBlockRequest] shouldBe None.orNull
      }
  }

  "should re-request next block if a previous one failed with inventory v1" in fixture {
    case FixtureParams(mbInventoryEvents, mbResponseEvents, loadEvents, _) =>
      val ch1 = new EmbeddedChannel()
      val ch2 = new EmbeddedChannel()

      val firstSign  = buildTestSign(1)
      val secondSign = buildTestSign(2)

      for {
        _ <- send(mbInventoryEvents)(ch1 -> MicroBlockInventoryV1(signer, secondSign, firstSign))
        _ = ch1.readOutbound[MicroBlockRequest] shouldBe MicroBlockRequest(secondSign)
        _ <- send(mbInventoryEvents)(ch2 -> MicroBlockInventoryV1(signer, secondSign, firstSign))
        _ <- Task.sleep(100.millis)
        _ = ch2.readOutbound[MicroBlockRequest] shouldBe MicroBlockRequest(secondSign)
        _              <- send(mbResponseEvents)(ch2 -> MicroBlockResponse(buildMicroBlock(secondSign, firstSign)))
        newMicroBlocks <- loadEvents
      } yield {
        newMicroBlocks.size shouldBe 1
        newMicroBlocks.head.channel shouldBe ch2
      }
  }

  "should re-request next block if a previous one failed with inventory v2" in fixture {
    case FixtureParams(mbInventoryEvents, mbResponseEvents, loadEvents, baseBlock) =>
      val ch1 = new EmbeddedChannel()
      val ch2 = new EmbeddedChannel()

      val firstSign  = buildTestSign(1)
      val secondSign = buildTestSign(2)

      for {
        _ <- send(mbInventoryEvents)(ch1 -> MicroBlockInventoryV2(baseBlock.uniqueId, signer, secondSign, firstSign))
        _ = ch1.readOutbound[MicroBlockRequest] shouldBe MicroBlockRequest(secondSign)
        _ <- send(mbInventoryEvents)(ch2 -> MicroBlockInventoryV2(baseBlock.uniqueId, signer, secondSign, firstSign))
        _ <- Task.sleep(100.millis)
        _ = ch2.readOutbound[MicroBlockRequest] shouldBe MicroBlockRequest(secondSign)
        _              <- send(mbResponseEvents)(ch2 -> MicroBlockResponse(buildMicroBlock(secondSign, firstSign)))
        newMicroBlocks <- loadEvents
      } yield {
        newMicroBlocks.size shouldBe 1
        newMicroBlocks.head.channel shouldBe ch2
      }
  }

  "should not request the same micro if received before with inventory v1" in fixture {
    case FixtureParams(mbInventoryEvents, mbResponseEvents, _, _) =>
      val ch = new EmbeddedChannel()

      val firstSign  = buildTestSign(1)
      val secondSign = buildTestSign(2)

      for {
        _ <- send(mbInventoryEvents)(ch -> MicroBlockInventoryV1(signer, secondSign, firstSign))
        _ = ch.readOutbound[MicroBlockRequest] shouldBe MicroBlockRequest(secondSign)
        _ <- send(mbResponseEvents)(ch  -> MicroBlockResponse(buildMicroBlock(secondSign, firstSign)))
        _ <- send(mbInventoryEvents)(ch -> MicroBlockInventoryV1(signer, secondSign, firstSign))
      } yield {
        Option(ch.readOutbound[MicroBlockRequest]) shouldBe None
      }
  }

  "should not request the same micro if received before with inventory v2" in fixture {
    case FixtureParams(mbInventoryEvents, mbResponseEvents, _, baseBlock) =>
      val ch = new EmbeddedChannel()

      val firstSign  = buildTestSign(1)
      val secondSign = buildTestSign(2)

      for {
        _ <- send(mbInventoryEvents)(ch -> MicroBlockInventoryV2(baseBlock.uniqueId, signer, secondSign, firstSign))
        _ = ch.readOutbound[MicroBlockRequest] shouldBe MicroBlockRequest(secondSign)
        _ <- send(mbResponseEvents)(ch  -> MicroBlockResponse(buildMicroBlock(secondSign, firstSign)))
        _ <- send(mbInventoryEvents)(ch -> MicroBlockInventoryV2(baseBlock.uniqueId, signer, secondSign, firstSign))
      } yield {
        Option(ch.readOutbound[MicroBlockRequest]) shouldBe None
      }
  }

  "should remember inventory to make a request later with inventory v1" in fixture {
    case FixtureParams(mbInventoryEvents, mbResponseEvents, _, _) =>
      val ch1 = new EmbeddedChannel()
      val ch2 = new EmbeddedChannel()

      val firstSign  = buildTestSign(1)
      val secondSign = buildTestSign(2)
      val thirdSign  = buildTestSign(3)

      for {
        _ <- send(mbInventoryEvents)(ch1 -> MicroBlockInventoryV1(signer, secondSign, firstSign))
        _ = ch1.readOutbound[MicroBlockRequest] shouldBe MicroBlockRequest(secondSign)
        _ <- send(mbInventoryEvents)(ch2 -> MicroBlockInventoryV1(signer, thirdSign, secondSign))
        _ <- send(mbResponseEvents)(ch1  -> MicroBlockResponse(buildMicroBlock(secondSign, firstSign)))
      } yield {
        ch2.readOutbound[MicroBlockRequest] shouldBe MicroBlockRequest(thirdSign)
      }
  }

  "should remember inventory to make a request later with inventory v2" in fixture {
    case FixtureParams(mbInventoryEvents, mbResponseEvents, _, baseBlock) =>
      val ch1 = new EmbeddedChannel()
      val ch2 = new EmbeddedChannel()

      val firstSign  = buildTestSign(1)
      val secondSign = buildTestSign(2)
      val thirdSign  = buildTestSign(3)

      for {
        _ <- send(mbInventoryEvents)(ch1 -> MicroBlockInventoryV2(baseBlock.uniqueId, signer, secondSign, firstSign))
        _ = ch1.readOutbound[MicroBlockRequest] shouldBe MicroBlockRequest(secondSign)
        _ <- send(mbInventoryEvents)(ch2 -> MicroBlockInventoryV2(baseBlock.uniqueId, signer, thirdSign, secondSign))
        _ <- send(mbResponseEvents)(ch1  -> MicroBlockResponse(buildMicroBlock(secondSign, firstSign)))
      } yield {
        ch2.readOutbound[MicroBlockRequest] shouldBe MicroBlockRequest(thirdSign)
      }
  }
}
