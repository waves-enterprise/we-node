package com.wavesenterprise.api.grpc.service

import akka.actor.ActorSystem
import akka.grpc.GrpcServiceException
import akka.grpc.internal.GrpcMetadataImpl
import akka.grpc.scaladsl.{Metadata, MetadataBuilder}
import akka.stream.scaladsl.Sink
import cats.implicits.catsSyntaxOptionId
import com.google.protobuf.ByteString
import com.wavesenterprise.TestSchedulers.apiComputationsScheduler
import com.wavesenterprise.api.grpc.utils.{ConnectionIdMaxLength, ConnectionIdMetadataKey, ErrorMessageMetadataKey}
import com.wavesenterprise.block.Block
import com.wavesenterprise.database.PrivacyState
import com.wavesenterprise.protobuf.service.messagebroker.{BlockchainEventsServicePowerApi, BlockchainEvent => PbBlockchainEvent}
import com.wavesenterprise.protobuf.service.util.events.{
  EventsFilter,
  TxTypeFilter,
  BlockSignature => PbBlockSignature,
  CurrentEvent => PbCurrentEvent,
  GenesisBlock => PbGenesisBlock,
  SubscribeOnRequest => PbSubscribeOnRequest
}
import com.wavesenterprise.settings.AuthorizationSettings
import com.wavesenterprise.settings.api.{BlockchainEventsServiceSettings, DisabledHistoryEventsBufferSettings}
import com.wavesenterprise.state.{AppendedBlockHistory, BlockAppended, Blockchain, BlockchainEvent, ByteStr, EventResult, MicroBlockAppended, NgState}
import com.wavesenterprise.transaction.BlockchainUpdater
import com.wavesenterprise.{AsyncTest, BlockchainEventGen, NoShrink, TestTime}
import monix.reactive.subjects.ConcurrentSubject
import monix.reactive.{Observable, OverflowStrategy}
import org.apache.commons.lang3.RandomStringUtils
import org.scalacheck.Gen
import org.scalamock.scalatest.MockFactory
import org.scalatest.{BeforeAndAfterAll, FreeSpec, Matchers}
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks
import squants.information.Megabytes

import scala.concurrent.Await
import scala.concurrent.duration._

class BlockchainEventsServiceImplSpec
    extends FreeSpec
    with Matchers
    with MockFactory
    with BeforeAndAfterAll
    with ScalaCheckPropertyChecks
    with BlockchainEventGen
    with NoShrink
    with AsyncTest {
  implicit val system: ActorSystem = ActorSystem("BlockchainEventsServiceImplSpec")

  trait BlockchainUpdaterMock extends Blockchain with BlockchainUpdater
  trait StateMock             extends Blockchain with PrivacyState

  private val blockchainUpdater = mock[BlockchainUpdaterMock]
  private val state             = mock[StateMock]
  private val ngState           = mock[NgState]

  private val settings     = BlockchainEventsServiceSettings(5, DisabledHistoryEventsBufferSettings)
  private val time         = new TestTime
  private val ownerAddress = accountGen.sample.get.toAddress
  private val apiKeyHash   = "5M7C14rf3TAaWscd8fHvU6Kqo97iJFpvFwyQ3Q6vfztS"
  private val authSetting  = AuthorizationSettings.ApiKey(apiKeyHash, apiKeyHash)

  private val startFromCurrentEvent = PbSubscribeOnRequest.StartFrom.CurrentEvent(PbCurrentEvent())
  private val startFromGenesis      = PbSubscribeOnRequest.StartFrom.GenesisBlock(PbGenesisBlock())
  private val emptyMetadata         = new GrpcMetadataImpl(new io.grpc.Metadata())

  "#subscribeOn" - {
    val eventService = BlockchainEventsServiceImpl(authSetting,
                                                   ownerAddress,
                                                   time,
                                                   settings,
                                                   Megabytes(5).toBytes.toInt,
                                                   blockchainUpdater,
                                                   state,
                                                   apiComputationsScheduler)

    "should return empty stream when there is no events" in {
      val request         = PbSubscribeOnRequest(startFrom = startFromCurrentEvent)
      val collectedEvents = subscribeAndCollectResult(List.empty, eventService, request)

      collectedEvents shouldBe Seq.empty
    }

    "should throw error when request is missing any field" in {
      val requestWithoutStartFrom = PbSubscribeOnRequest()
      (the[GrpcServiceException] thrownBy {
        subscribeAndCollectResult(List.empty, eventService, requestWithoutStartFrom)
      }).metadata.getText(ErrorMessageMetadataKey) shouldBe Some("Missing required field 'start_from'")

      val startFrom                    = PbSubscribeOnRequest.StartFrom.BlockSignature(PbBlockSignature())
      val requestWithoutBlockSignature = PbSubscribeOnRequest(startFrom = startFrom)

      (the[GrpcServiceException] thrownBy {
        subscribeAndCollectResult(List.empty, eventService, requestWithoutBlockSignature, mockBlockchainUpdaterTimes = 0)
      }).metadata.getText(ErrorMessageMetadataKey) shouldBe Some("Missing required field 'last_block_signature'")
    }

    s"should throw error when '${ConnectionIdMetadataKey}' metadata key is too long" in {
      val request      = PbSubscribeOnRequest(startFrom = startFromCurrentEvent)
      val connectionId = RandomStringUtils.randomAlphanumeric(ConnectionIdMaxLength + 1).toLowerCase
      val metadata     = new MetadataBuilder().addText(ConnectionIdMetadataKey, connectionId).build()

      (the[GrpcServiceException] thrownBy {
        subscribeAndCollectResult(List.empty, eventService, request, Some(metadata), 0)
      }).metadata.getText(ErrorMessageMetadataKey) shouldBe Some {
        s"'$ConnectionIdMetadataKey' metadata value max length is 16. Got '${ConnectionIdMaxLength + 1}'"
      }
    }

    "should return events from Genesis in the same order" in {
      forAll(randomEventResultsGen(10)) { events =>
        val request         = PbSubscribeOnRequest(startFrom = startFromCurrentEvent)
        val expectedEvents  = prepareExpectedEvents(events)
        val collectedEvents = subscribeAndCollectResult(events, eventService, request)

        expectedEvents should contain theSameElementsInOrderAs collectedEvents
      }
    }

    "should return events starting Genesis in the same order" in {
      forAll(Gen.listOfN(10, randomSignerBlockGen).map(_.zipWithIndex), randomEventResultsGen(20)) { (indexedHistoryBlocks, events) =>
        mockGenesis(indexedHistoryBlocks)

        val historyEvents   = blocksToHistoryEvents(indexedHistoryBlocks)
        val request         = PbSubscribeOnRequest(startFrom = startFromGenesis)
        val expectedEvents  = eventsToProto(historyEvents) ++ prepareExpectedEvents(events)
        val collectedEvents = subscribeAndCollectResult(events, eventService, request)

        expectedEvents should contain theSameElementsInOrderAs collectedEvents
      }
    }

    "should return with history events in the same order" in {
      forAll(Gen.listOfN(10, randomSignerBlockGen).map(_.zipWithIndex), randomEventResultsGen(20), randomSignerBlockGen) {
        (indexedHistoryBlocks, events, returnToBlock) =>
          mockHistoryEvents(returnToBlock.uniqueId, indexedHistoryBlocks)

          val historyEvents   = blocksToHistoryEvents(indexedHistoryBlocks, 2)
          val request         = PbSubscribeOnRequest(startFrom = startFromSignature(returnToBlock))
          val expectedEvents  = eventsToProto(historyEvents) ++ prepareExpectedEvents(events)
          val collectedEvents = subscribeAndCollectResult(events, eventService, request)

          expectedEvents should contain theSameElementsInOrderAs collectedEvents
      }
    }

    "should return with history events and tx type filter in the same order" in {
      forAll(Gen.listOfN(10, randomSignerBlockGen).map(_.zipWithIndex), randomEventResultsGen(20), randomSignerBlockGen) {
        (indexedHistoryBlocks, events, returnToBlock) =>
          mockHistoryEvents(returnToBlock.uniqueId, indexedHistoryBlocks)

          val historyEvents   = blocksToHistoryEvents(indexedHistoryBlocks, 2)
          val eventsFilter    = filterInByTypes(Seq(10))
          val request         = PbSubscribeOnRequest(startFromSignature(returnToBlock), eventsFilter :: Nil)
          val expectedEvents  = eventsToProto(historyEvents, eventsFilter :: Nil) ++ prepareExpectedEvents(events, eventsFilter :: Nil)
          val collectedEvents = subscribeAndCollectResult(events, eventService, request)

          expectedEvents should contain theSameElementsInOrderAs collectedEvents
      }
    }

    "should return with history events and tx type filterNot in the same order" in {
      forAll(Gen.listOfN(10, randomSignerBlockGen).map(_.zipWithIndex), randomEventResultsGen(20), randomSignerBlockGen) {
        (indexedHistoryBlocks, events, returnToBlock) =>
          mockHistoryEvents(returnToBlock.uniqueId, indexedHistoryBlocks)

          val historyEvents   = blocksToHistoryEvents(indexedHistoryBlocks, 2)
          val eventsFilter    = filterOutByTypes(Seq(10))
          val request         = PbSubscribeOnRequest(startFromSignature(returnToBlock), eventsFilter :: Nil)
          val expectedEvents  = eventsToProto(historyEvents, eventsFilter :: Nil) ++ prepareExpectedEvents(events, eventsFilter :: Nil)
          val collectedEvents = subscribeAndCollectResult(events, eventService, request)

          expectedEvents should contain theSameElementsInOrderAs collectedEvents
      }
    }

    "should skip first MicroBlockAppend events when ngState is None" in {
      forAll(randomEventResultsGen(10), Gen.chooseNum(1, 5).flatMap(i => Gen.listOfN(i, microBlockAppendedGen()))) { (events, microBlockHeadEvents) =>
        val request         = PbSubscribeOnRequest(startFrom = startFromCurrentEvent)
        val allEvents       = microBlockHeadEvents ++ events
        val expectedEvents  = prepareExpectedEvents(allEvents, maybeNgState = None)
        val collectedEvents = subscribeAndCollectResult(allEvents, eventService, request)

        expectedEvents should contain theSameElementsInOrderAs collectedEvents
      }
    }

    "should throw error when history event not found" in {
      forAll(Gen.listOfN(5, randomSignerBlockGen).map(_.zipWithIndex), randomEventResultsGen(20), randomSignerBlockGen) {
        (indexedHistoryBlocks, events, returnToBlock) =>
          val maxHistoryEventsCount = 3
          mockHistoryEvents(returnToBlock.uniqueId, indexedHistoryBlocks, maxHistoryEventsCount)

          val request = PbSubscribeOnRequest(startFrom = startFromSignature(returnToBlock))

          (the[GrpcServiceException] thrownBy {
            subscribeAndCollectResult(events, eventService, request, mockBlockchainUpdaterTimes = 1)
          }).metadata.getText(ErrorMessageMetadataKey) shouldBe Some(s"block at height '${maxHistoryEventsCount + 2}' does not exist")
      }
    }
  }

  private def startFromSignature(returnToBlock: Block): PbSubscribeOnRequest.StartFrom.BlockSignature =
    PbSubscribeOnRequest.StartFrom.BlockSignature(PbBlockSignature(ByteString.copyFrom(returnToBlock.uniqueId.arr).some))

  private def filterInByTypes(acceptableTxTypes: Seq[Int]): EventsFilter =
    EventsFilter(
      EventsFilter.FilterType.FilterIn(EventsFilter.FilterIn()),
      EventsFilter.EventsFilter.TxTypeFilter(TxTypeFilter(acceptableTxTypes))
    )

  private def filterOutByTypes(unacceptableTxTypes: Seq[Int]): EventsFilter =
    EventsFilter(
      EventsFilter.FilterType.FilterOut(EventsFilter.FilterOut()),
      EventsFilter.EventsFilter.TxTypeFilter(TxTypeFilter(unacceptableTxTypes))
    )

  private def subscribeAndCollectResult(events: Seq[BlockchainEvent],
                                        eventService: BlockchainEventsServicePowerApi,
                                        request: PbSubscribeOnRequest,
                                        metaOpt: Option[Metadata] = None,
                                        mockBlockchainUpdaterTimes: Int = 2) = {
    val subject          = ConcurrentSubject.publish[BlockchainEvent]
    val eventsObservable = subject.asyncBoundary(OverflowStrategy.Default)
    (blockchainUpdater.lastBlockchainEvent _).expects.returns(eventsObservable).repeated(mockBlockchainUpdaterTimes)

    val source = metaOpt match {
      case Some(meta) => eventService.subscribeOn(request, meta)
      case None       => eventService.subscribeOn(request, emptyMetadata)
    }

    val publisher   = source.runWith(Sink.asPublisher[PbBlockchainEvent](fanout = false))
    val collectTask = Observable.fromReactivePublisher(publisher).toListL.executeAsync.runToFuture

    Thread.sleep(2.seconds.toMillis) // wait until history eventsWithRollback processed
    events.foreach { e =>
      subject.onNext(e)
      Thread.sleep(50)
    }
    Thread.sleep(2.seconds.toMillis)
    subject.onComplete()

    Await.result(collectTask, 10.seconds)
  }

  override protected def afterAll(): Unit = {
    Await.result(system.terminate(), 10 seconds)
    super.afterAll()
  }

  private def mockHistoryEvents(returnToBlockSig: ByteStr,
                                indexedHistoryBlocks: Seq[(Block, Int)],
                                maxHistoryEventsCount: Int = Integer.MAX_VALUE): Unit = {
    (state.heightOf _).expects(returnToBlockSig).returns(1.some)
    (state.height _).expects.returns(indexedHistoryBlocks.size + 1).repeated(maxHistoryEventsCount.min(indexedHistoryBlocks.size) + 1)
    indexedHistoryBlocks.take(maxHistoryEventsCount).foreach {
      case (block, index) => (state.blockBytes(_: Int)).expects(index + 2).returns(block.bytes().some)
    }
    if (indexedHistoryBlocks.size > maxHistoryEventsCount) {
      (state.blockBytes(_: Int)).expects(maxHistoryEventsCount + 2).returns(None)
    }
  }

  private def mockGenesis(indexedHistoryBlocks: Seq[(Block, Int)]): Unit = {
    (state.height _).expects.returns(indexedHistoryBlocks.size).repeated(indexedHistoryBlocks.size + 1)
    indexedHistoryBlocks.foreach {
      case (block, index) => (state.blockBytes(_: Int)).expects(index + 1).returns(block.bytes().some)
    }
  }

  private def prepareExpectedEvents(events: List[BlockchainEvent],
                                    eventsFilters: Seq[EventsFilter] = Nil,
                                    maybeNgState: Option[NgState] = ngState.some): Seq[PbBlockchainEvent] =
    getFirstEvent(events, maybeNgState)
      .map { case (head, index) => eventsToProto(head :: events.drop(index + 1), eventsFilters) }
      .getOrElse(Seq.empty[PbBlockchainEvent])

  private def getFirstEvent(events: List[BlockchainEvent], maybeNgState: Option[NgState]): Option[(BlockchainEvent, Int)] =
    events.zipWithIndex.toStream
      .map {
        case (ba: BlockAppended, index) => ba.toHistoryEvent.some -> index
        case (head: MicroBlockAppended, index) =>
          (blockchainUpdater.ngState _).expects.returns(maybeNgState)
          maybeNgState
            .map { ngState =>
              (ngState.transactions _).expects.returns(head.transactions)
              head.some -> index
            }
            .getOrElse(None -> index)
        case (other, index) => other.some -> index
      }
      .collectFirst {
        case (Some(event), i) => event -> i
      }

  private def eventsToProto(events: List[BlockchainEvent], eventsFilters: Seq[EventsFilter] = Nil): Seq[PbBlockchainEvent] = {
    val txTypesPredicate = BlockchainEventsServiceImpl.getTxPredicate(eventsFilters)
    events
      .map {
        case eventResult: EventResult => eventResult.withTransactions(eventResult.transactions.filter(txTypesPredicate))
        case other                    => other
      }
      .map(_.toProto)
  }

  private def blocksToHistoryEvents(indexedBlocks: List[(Block, Int)], startFromHeight: Int = 1): List[AppendedBlockHistory] =
    indexedBlocks.map {
      case (block, index) =>
        AppendedBlockHistory(
          block.uniqueId,
          block.reference,
          block.transactionData,
          block.signerData.generator.toAddress.bytes,
          index + startFromHeight,
          block.version,
          block.timestamp,
          block.blockFee(),
          block.bytes().length,
          block.featureVotes
        )
    }
}
