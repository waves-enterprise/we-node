package com.wavesenterprise.network

import com.wavesenterprise.account.PrivateKeyAccount
import com.wavesenterprise.block.{Block, SignerData}
import com.wavesenterprise.lagonaki.mocks.TestBlock
import com.wavesenterprise.network.BlockLoader.ExtensionBlocks
import com.wavesenterprise.network.SyncChannelSelector.{ScoredChannelInfo, SyncChannelUpdateEvent}
import com.wavesenterprise.state.ByteStr
import com.wavesenterprise.transaction.ValidationError.GenericError
import com.wavesenterprise.{BlockGen, RxSetup, TransactionGen}
import io.netty.channel.Channel
import io.netty.channel.embedded.EmbeddedChannel
import io.netty.channel.local.LocalChannel
import monix.eval.{Coeval, Task}
import monix.reactive.subjects.PublishSubject
import org.scalamock.scalatest.MockFactory
import org.scalatest.{Assertion, FreeSpec, Matchers}
import tools.GenHelper._

import scala.concurrent.duration._

class BlockLoaderSpec extends FreeSpec with Matchers with TransactionGen with RxSetup with BlockGen with MockFactory {

  val MaxRollback = 10

  val test100Signs: Seq[ByteStr]   = Seq.tabulate(100)(buildTestSign)
  val smallTimeout: FiniteDuration = 1.millis
  val midTimeout: FiniteDuration   = 1.second

  val signer: PrivateKeyAccount = TestBlock.defaultSigner

  def buildTestBlock(id: Int): Block = {
    val block          = TestBlock.create(Seq.empty)
    val newBlockHeader = block.blockHeader.copy(signerData = SignerData(signer, buildTestSign(id)))
    block.copy(blockHeader = newBlockHeader)
  }

  case class FixtureParams(invalidBlockStorage: InvalidBlockStorage,
                           incomingBlockEvents: PublishSubject[(Channel, Block)],
                           incomingSignatureEvents: PublishSubject[(Channel, Signatures)],
                           syncChannelUpdateEvents: PublishSubject[SyncChannelUpdateEvent],
                           newBlocksEventLoader: Task[Seq[(Channel, Block)]],
                           newExtensionsEventLoader: Task[Seq[(Channel, ExtensionBlocks)]])

  private def fixture(
      lastBlockIds: Seq[ByteStr] = Seq.empty,
      timeOut: FiniteDuration = 1.day
  )(testBlock: FixtureParams => Task[Assertion]): Assertion = {

    val invalidBlockStorage     = new InMemoryInvalidBlockStorage
    val incomingBlockEvents     = PublishSubject[(Channel, Block)]
    val incomingSignatureEvents = PublishSubject[(Channel, Signatures)]
    val syncChannelUpdateEvents = PublishSubject[SyncChannelUpdateEvent]
    val channelClosedEvents     = PublishSubject[Channel]
    val incomingMissingBlocks   = PublishSubject[(Channel, MissingBlock)]

    val blockLoader = new BlockLoader(
      syncTimeOut = timeOut,
      extensionBatchSize = 10,
      lastBlockIdsReporter = Coeval(lastBlockIds.reverse.take(MaxRollback)),
      invalidBlocks = invalidBlockStorage,
      incomingBlockEvents = incomingBlockEvents,
      incomingSignatureEvents = incomingSignatureEvents,
      incomingMissingBlockEvents = incomingMissingBlocks,
      syncChannelUpdateEvents = syncChannelUpdateEvents,
      channelCloseEvents = channelClosedEvents
    )

    val newBlocksEventLoader: Task[Seq[(Channel, Block)]] = {
      loadEvents(blockLoader.broadcastBlockEvents)
    }

    val newExtensionsEventLoader: Task[Seq[(Channel, ExtensionBlocks)]] = {
      loadEvents(blockLoader.extensionEvents)
    }

    val params =
      FixtureParams(invalidBlockStorage,
                    incomingBlockEvents,
                    incomingSignatureEvents,
                    syncChannelUpdateEvents,
                    newBlocksEventLoader,
                    newExtensionsEventLoader)

    try {
      test(testBlock(params))
    } finally {
      incomingBlockEvents.onComplete()
      incomingSignatureEvents.onComplete()
      syncChannelUpdateEvents.onComplete()
    }
  }

  "should propagate unexpected block" in fixture() {
    case FixtureParams(_, incomingBlockEvents, _, _, loadBlocksEvent, _) =>
      val channel = new LocalChannel()
      val block   = randomSignerBlockGen.generateSample()

      for {
        _         <- send(incomingBlockEvents)(channel -> block)
        newBlocks <- loadBlocksEvent
      } yield {
        newBlocks.last shouldBe (channel -> block)
      }
  }

  "should blacklist node when request signatures timeout" in fixture(test100Signs, smallTimeout) {
    case FixtureParams(_, _, _, syncChannelUpdateEvents, _, _) =>
      val ch                 = new EmbeddedChannel()
      val channelUpdateEvent = SyncChannelUpdateEvent(Some(ScoredChannelInfo(ch, 1)))

      for {
        _ <- send(syncChannelUpdateEvents)(channelUpdateEvent)
        _ <- Task.sleep(smallTimeout * 10)
      } yield {
        ch.isOpen shouldBe false
      }
  }

  "should request signatures and then span blocks from peer" in fixture(test100Signs) {
    case FixtureParams(_, _, incomingSignatureEvents, syncChannelUpdateEvents, _, _) =>
      val ch                     = new EmbeddedChannel()
      val syncChannelUpdateEvent = SyncChannelUpdateEvent(Some(ScoredChannelInfo(ch, 1)))

      for {
        _ <- send(syncChannelUpdateEvents)(syncChannelUpdateEvent)
        expectedRequestSignatures = test100Signs.reverse.take(MaxRollback)
        _                         = ch.readOutbound[GetNewSignatures].knownSignatures shouldBe expectedRequestSignatures
        newSignaturesResponse     = Signatures(Seq(100, 101).map(buildTestSign))
        _ <- send(incomingSignatureEvents)(ch -> newSignaturesResponse)
      } yield {
        ch.readOutbound[GetBlock].signature shouldBe buildTestSign(100)
        ch.readOutbound[GetBlock].signature shouldBe buildTestSign(101)
      }
  }

  "should blacklist if received signatures contains banned id" in fixture(test100Signs) {
    case FixtureParams(invalidBlockStorage, _, incomingSignatureEvents, syncChannelUpdateEvents, _, _) =>
      invalidBlockStorage.add(buildTestSign(105), GenericError("Some error"))
      val ch                     = new EmbeddedChannel()
      val syncChannelUpdateEvent = SyncChannelUpdateEvent(Some(ScoredChannelInfo(ch, 1)))

      for {
        _ <- send(syncChannelUpdateEvents)(syncChannelUpdateEvent)
        _                     = ch.readOutbound[GetNewSignatures].knownSignatures.size shouldBe MaxRollback
        newSignaturesResponse = Signatures(Range(99, 110).map(buildTestSign))
        _ <- send(incomingSignatureEvents)(ch -> newSignaturesResponse)
      } yield {
        ch.isOpen shouldBe false
      }
  }

  "should blacklist if some blocks didn't arrive in due time" in fixture(test100Signs, midTimeout) {
    case FixtureParams(_, incomingBlockEvents, incomingSignatureEvents, syncChannelUpdateEvents, _, _) =>
      val ch                     = new EmbeddedChannel()
      val syncChannelUpdateEvent = SyncChannelUpdateEvent(Some(ScoredChannelInfo(ch, 1)))

      for {
        _ <- send(syncChannelUpdateEvents)(syncChannelUpdateEvent)
        _                     = ch.readOutbound[GetNewSignatures].knownSignatures.size shouldBe MaxRollback
        newSignaturesResponse = Signatures(Seq(100, 101).map(buildTestSign))
        _ <- send(incomingSignatureEvents)(ch -> newSignaturesResponse)
        _ = ch.readOutbound[GetBlock].signature shouldBe buildTestSign(100)
        _ = ch.readOutbound[GetBlock].signature shouldBe buildTestSign(101)
        _ <- send(incomingBlockEvents)(ch -> buildTestBlock(100))
        _ = ch.isOpen shouldBe true
        _ <- Task.sleep(midTimeout + 10.millis)
      } yield {
        ch.isOpen shouldBe false
      }
  }

  "should apply received extension" in fixture(test100Signs) {
    case FixtureParams(_, incomingBlockEvents, incomingSignatureEvents, syncChannelUpdateEvents, _, loadNewExtensions) =>
      val ch                     = new EmbeddedChannel()
      val syncChannelUpdateEvent = SyncChannelUpdateEvent(Some(ScoredChannelInfo(ch, 1)))

      val testBlock1 = buildTestBlock(100)
      val testBlock2 = buildTestBlock(101)

      for {
        _ <- send(syncChannelUpdateEvents)(syncChannelUpdateEvent)
        _                     = ch.readOutbound[GetNewSignatures].knownSignatures.size shouldBe MaxRollback
        newSignaturesResponse = Signatures(Seq(testBlock1.uniqueId, testBlock2.uniqueId))
        _ <- send(incomingSignatureEvents)(ch -> newSignaturesResponse)
        _ = ch.readOutbound[GetBlock].signature shouldBe testBlock1.uniqueId
        _ = ch.readOutbound[GetBlock].signature shouldBe testBlock2.uniqueId
        _             <- send(incomingBlockEvents)(ch -> testBlock1)
        _             <- send(incomingBlockEvents)(ch -> testBlock2)
        newExtensions <- loadNewExtensions
      } yield {
        val (_, extension) = newExtensions.head
        extension.blocks shouldBe Seq(testBlock1, testBlock2)
      }
  }
}
