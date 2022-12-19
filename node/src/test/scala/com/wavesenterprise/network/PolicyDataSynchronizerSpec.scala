package com.wavesenterprise.network

import cats.implicits._
import com.wavesenterprise.account.{Address, PrivateKeyAccount}
import com.wavesenterprise.database.PrivacyState
import com.wavesenterprise.features.BlockchainFeature
import com.wavesenterprise.network.peers.{ActivePeerConnections, PeerConnection, PeerInfo, PeerSession}
import com.wavesenterprise.network.privacy.{EnablePolicyDataSynchronizer, PolicyDataReplier, PolicyStrictDataCache, PrivacyInventoryHandler}
import com.wavesenterprise.privacy.{PolicyDataHash, PolicyDataId, PolicyStorage}
import com.wavesenterprise.settings.PositiveInt
import com.wavesenterprise.settings.privacy.{PolicyDataCacheSettings, PrivacyInventoryHandlerSettings, PrivacySynchronizerSettings}
import com.wavesenterprise.state.{Blockchain, ByteStr}
import com.wavesenterprise.transaction.{BlockchainUpdater, CreatePolicyTransaction, PolicyUpdate}
import com.wavesenterprise.utils.EitherUtils.EitherExt
import com.wavesenterprise.utils.ScorexLogging
import com.wavesenterprise.{NodeVersion, TestTime, TransactionGen}
import io.netty.channel.Channel
import io.netty.channel.local.LocalChannel
import monix.eval.Task
import monix.execution.schedulers.SchedulerService
import monix.execution.{Ack, Scheduler, UncaughtExceptionReporter}
import monix.reactive.subjects.ConcurrentSubject
import org.scalamock.scalatest.MockFactory
import org.scalatest.concurrent.ScalaFutures
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks
import tools.GenHelper._

import java.net.SocketAddress
import java.util.concurrent.{ConcurrentHashMap, ConcurrentLinkedQueue, RejectedExecutionException}
import scala.annotation.tailrec
import scala.collection.JavaConverters._
import scala.concurrent.Future
import scala.concurrent.duration._
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers

class PolicyDataSynchronizerSpec
    extends AnyFreeSpec
    with Matchers
    with ScalaCheckPropertyChecks
    with ScalaFutures
    with MockFactory
    with TransactionGen
    with ScorexLogging {

  override implicit val patienceConfig: PatienceConfig = PatienceConfig(timeout = 15 seconds, interval = 300 millis)

  private[this] val cache = new PolicyStrictDataCache(PolicyDataCacheSettings(100, 10.minute))

  trait TestState extends Blockchain with BlockchainUpdater with PrivacyState

  case class FixtureParams(
      state: TestState,
      storage: PolicyStorage,
      policyId: ByteStr,
      initDataWithTxs: List[PolicyDataWithTxV1],
      additionalDataWithTxs: List[PolicyDataWithTxV1],
      policyUpdates: ConcurrentSubject[PolicyUpdate, PolicyUpdate],
      policyRollbacks: ConcurrentSubject[PolicyDataId, PolicyDataId],
      peers: ActivePeerConnections,
      recipientAccount: PrivateKeyAccount,
      scheduler: Scheduler
  )

  def fixture(test: FixtureParams => Unit): Unit = {
    implicit val scheduler: SchedulerService = Scheduler.singleThread("PolicyDataSynchronizerScheduler",
                                                                      reporter = {
                                                                        case _: RejectedExecutionException => // ignore
                                                                        case ex                            => UncaughtExceptionReporter.default.reportFailure(ex)
                                                                      })

    val initTxCount = 2
    val (CreatePolicyTransactionV1TestWrap(policyTx, _), allDataWithTx) = createPolicyAndDataHashTransactionsV1Gen(initTxCount + 2)
      .generateSample()

    val initPendingDataHashTxs = allDataWithTx.take(initTxCount)
    val additionalDataHashTxs  = allDataWithTx.drop(initTxCount)
    val policyUpdates          = ConcurrentSubject.publish[PolicyUpdate]
    val policyRollbacks        = ConcurrentSubject.publish[PolicyDataId]
    val storage                = TestPolicyStorage.build()
    val recipientAccount       = PrivateKeyAccount(com.wavesenterprise.crypto.generateKeyPair())
    val state                  = buildTestPolicyState(policyTx, initPendingDataHashTxs, policyUpdates, policyRollbacks, recipientAccount)
    val peers = new ActivePeerConnections(100) {
      override def flushWrites(): Unit = ()
    }

    val fixtureParams = FixtureParams(
      state,
      storage,
      policyTx.id(),
      initPendingDataHashTxs,
      additionalDataHashTxs,
      policyUpdates,
      policyRollbacks,
      peers,
      recipientAccount,
      scheduler
    )

    try {
      test(fixtureParams)
    } finally {
      scheduler.shutdown()
    }
  }

  "Start and stop crawling tasks" in fixture {
    case FixtureParams(state,
                       storage,
                       policyId,
                       initPendingDataHashTxs,
                       additionalDataHashTxs,
                       policyUpdates,
                       policyRollbacks,
                       peers,
                       _,
                       scheduler) =>
      implicit val implicitScheduler: Scheduler = scheduler

      // Rollback stream should not depend on crawling parallelism
      val parallelism = PositiveInt(initPendingDataHashTxs.size + additionalDataHashTxs.size)
      val settings = new PrivacySynchronizerSettings(
        requestTimeout = 1.second,
        initRetryDelay = 1.second,
        inventoryStreamTimeout = 1.seconds,
        inventoryRequestDelay = 3.seconds,
        inventoryTimestampThreshold = 10.minutes,
        crawlingParallelism = parallelism,
        maxAttemptCount = PositiveInt(20),
        lostDataProcessingDelay = 1.minute,
        networkStreamBufferSize = PositiveInt(3),
        failedPeersCacheSize = 100,
        failedPeersCacheExpireTimeout = 5.minutes
      )
      val owner             = accountGen.generateSample()
      val responses         = ConcurrentSubject.publish[(Channel, PrivateDataResponse)]
      val inventories       = ConcurrentSubject.publish[(Channel, PrivacyInventoryV1)]
      val inventoryRequests = ConcurrentSubject.publish[(Channel, PrivacyInventoryRequest)]
      val privacyInventoryHandlerSettings =
        PrivacyInventoryHandlerSettings(500.millis, PositiveInt(500), PositiveInt(100), 5.minutes, PositiveInt(10))
      val privacyInventoryHandler =
        new PrivacyInventoryHandler(inventories, inventoryRequests, owner, peers, state, storage, privacyInventoryHandlerSettings)
      val synchronizer = new EnablePolicyDataSynchronizer(
        state = state,
        owner = owner,
        settings = settings,
        responses = responses,
        privacyInventoryHandler = privacyInventoryHandler,
        peers = peers,
        maxSimultaneousConnections = 30,
        storage = storage,
        strictDataCache = cache,
        time = new TestTime()
      )

      val fibers = synchronizer.internalRun()

      waitUntil(fibers.size == initPendingDataHashTxs.size, "Failed to wait until all init crawling tasks started")

      initPendingDataHashTxs.foreach {
        case PolicyDataWithTxV1(_, tx) =>
          fibers.keySet should contain(PolicyDataId(policyId, tx.dataHash))
      }

      updateState(state, policyUpdates, additionalDataHashTxs)

      additionalDataHashTxs.foreach {
        case PolicyDataWithTxV1(_, tx) =>
          val key = PolicyDataId(policyId, tx.dataHash)
          state.pendingPrivacyItems() should contain(key)
          fibers.keySet should contain(key)
      }

      rollbackState(state, policyRollbacks, additionalDataHashTxs)

      additionalDataHashTxs.foreach {
        case PolicyDataWithTxV1(_, tx) =>
          val key = PolicyDataId(policyId, tx.dataHash)
          state.pendingPrivacyItems() shouldNot contain(key)
          fibers.keySet shouldNot contain(key)
      }
  }

  "Get and decrypt response" in fixture {
    case FixtureParams(state,
                       storage,
                       policyId,
                       initPendingDataHashTxs,
                       additionalDataHashTxs,
                       policyUpdates,
                       _,
                       peers,
                       recipientAccount,
                       scheduler) =>
      implicit val implicitScheduler: Scheduler = scheduler

      val parallelism        = PositiveInt(initPendingDataHashTxs.size * 2) // Pending items processing takes 50% parallelism
      val inventoryThreshold = 10.minutes

      val settings = new PrivacySynchronizerSettings(
        requestTimeout = 5.second,
        initRetryDelay = 5.second,
        inventoryStreamTimeout = 5.seconds,
        inventoryRequestDelay = 1.seconds,
        inventoryTimestampThreshold = inventoryThreshold,
        crawlingParallelism = parallelism,
        maxAttemptCount = PositiveInt(20),
        lostDataProcessingDelay = 1.minute,
        networkStreamBufferSize = PositiveInt(3),
        failedPeersCacheSize = 100,
        failedPeersCacheExpireTimeout = 5.minutes
      )

      val owner             = accountGen.generateSample()
      val responses         = ConcurrentSubject.publish[(Channel, PrivateDataResponse)]
      val inventories       = ConcurrentSubject.publish[(Channel, PrivacyInventoryV1)]
      val inventoryRequests = ConcurrentSubject.publish[(Channel, PrivacyInventoryRequest)]
      val privacyInventoryHandlerSettings =
        PrivacyInventoryHandlerSettings(500.millis, PositiveInt(500), PositiveInt(100), 5.minutes, PositiveInt(10))
      val privacyInventoryHandler =
        new PrivacyInventoryHandler(inventories, inventoryRequests, owner, peers, state, storage, privacyInventoryHandlerSettings)

      val peerRequests = new ConcurrentLinkedQueue[Object]()

      val synchronizer =
        new EnablePolicyDataSynchronizer(
          state = state,
          owner = owner,
          settings = settings,
          responses = responses,
          privacyInventoryHandler = privacyInventoryHandler,
          peers = peers,
          maxSimultaneousConnections = 30,
          storage = storage,
          strictDataCache = cache,
          time = new TestTime()
        )(scheduler) {
          override def selectPeers(policyRecipients: Set[Address], policyId: ByteStr): Vector[PeerSession] =
            Vector(mock[PeerSession])

          override protected def sendToPeers(request: Object, peers: PeerSession*): Task[Unit] =
            Task(peerRequests.add(request)).void

          override def broadcastInventory(inventory: PrivacyInventory, flushChannels: Boolean, excludeChannels: Set[Channel]): Unit =
            peerRequests.add(inventory)
        }

      privacyInventoryHandler.run()

      val fibers = synchronizer.internalRun()

      // Additional txs are sent with timestamp. Even txs with outdated timestamp, odd – with actual ts.
      val additionalTxIdToInventoryTs = additionalDataHashTxs.zipWithIndex.map {
        case (data, i) =>
          data.tx.id() ->
            (if (i % 2 == 0)
               data.tx.timestamp - (inventoryThreshold.toMillis * 2)
             else
               data.tx.timestamp)
      }.toMap

      val additionalTxIdsWithOldTs = additionalDataHashTxs.view.zipWithIndex.flatMap {
        case (data, i) if i % 2 == 0 => Some(data.tx.id())
        case _ => None
      }.toSet

      updateState(state, policyUpdates, additionalDataHashTxs, additionalTxIdToInventoryTs)

      val allDataHashTxs = initPendingDataHashTxs ++ additionalDataHashTxs

      val channel         = new LocalChannel()
      val localSessionKey = PrivateKeyAccount(com.wavesenterprise.crypto.generateSessionKeyPair())
      val peerSessionKey  = PrivateKeyAccount(com.wavesenterprise.crypto.generateSessionKeyPair())
      val peerAddress     = recipientAccount.toAddress
      val peerInfo = PeerInfo(
        remoteAddress = mock[SocketAddress],
        declaredAddress = None,
        chainId = 'I',
        nodeVersion = NodeVersion(1, 8, 0),
        applicationConsensus = "PoS",
        nodeName = "node",
        nodeNonce = 0L,
        nodeOwnerAddress = peerAddress,
        sessionPubKey = peerSessionKey
      )

      allDataHashTxs.foreach {
        case PolicyDataWithTxV1(data, tx) =>
          peers.putIfAbsentAndMaxNotReachedOrReplaceValidator(new PeerConnection(channel, peerInfo, localSessionKey))

          val key = PolicyDataId(policyId, tx.dataHash)
          waitUntil(fibers.contains(key), s"Failed to wait until the '$key' task started")
          state.pendingPrivacyItems() should contain(key)

          waitUntil(peerRequests.contains(PrivacyInventoryRequest(policyId, tx.dataHash)), s"Failed to wait inventory request for key '$key'")

          val inventory = PrivacyInventoryV1(recipientAccount, policyId, tx.dataHash)
          inventories.onNext(channel -> inventory)

          waitUntil(peerRequests.contains(PrivateDataRequest(policyId, tx.dataHash)), s"Failed to wait data request for key '$key'")

          val metaData = policyMetaData.generateSample().copy(policyId = policyId.toString, hash = tx.dataHash.toString)
          val encrypted = PolicyDataReplier
            .encryptStrictResponse(peers, channel, policyId, tx.dataHash, data, metaData)
            .explicitGet()

          responses.onNext(channel -> encrypted)
      }

      waitUntil(fibers.isEmpty, "Failed to wait until all running tasks completes")

      allDataHashTxs.foreach {
        case PolicyDataWithTxV1(data, tx) =>
          val key = PolicyDataId(tx.policyId, tx.dataHash)
          state.pendingPrivacyItems shouldNot contain(key)
          fibers.keySet shouldNot contain(key)
          storage.policyItemData(tx.policyId.toString, tx.dataHash.toString).runToFuture.futureValue shouldBe Right(Some(ByteStr(data)))
      }

      val sentInventories = peerRequests.asScala.collect {
        case inv: PrivacyInventory => inv.policyId -> inv.dataHash
      }.toSet

      // Initial txs come in without timestamp.
      initPendingDataHashTxs.foreach {
        case PolicyDataWithTxV1(_, tx) =>
          sentInventories shouldNot contain(tx.policyId -> tx.dataHash)
      }

      // Additional txs come in with timestamp.
      additionalDataHashTxs.foreach {
        case PolicyDataWithTxV1(_, tx) =>
          if (additionalTxIdsWithOldTs.contains(tx.id())) {
            sentInventories shouldNot contain(tx.policyId -> tx.dataHash)
          } else {
            sentInventories should contain(tx.policyId -> tx.dataHash)
          }
      }
  }

  "Mark item as lost after maximum number of retries and start processing it" in fixture {
    case FixtureParams(state, storage, _, initPendingDataHashTxs, additionalDataHashTxs, policyUpdates, _, peers, _, scheduler) =>
      implicit val implicitScheduler: Scheduler = scheduler

      val settings = new PrivacySynchronizerSettings(
        requestTimeout = 1.second,
        initRetryDelay = 1.millis,
        inventoryStreamTimeout = 1.seconds,
        inventoryRequestDelay = 3.seconds,
        inventoryTimestampThreshold = 10.minutes,
        crawlingParallelism = PositiveInt((initPendingDataHashTxs.size + additionalDataHashTxs.size) * 4), // Lost items processing takes 25% parallelism
        maxAttemptCount = PositiveInt(2),
        lostDataProcessingDelay = 1.second,
        networkStreamBufferSize = PositiveInt(3),
        failedPeersCacheSize = 100,
        failedPeersCacheExpireTimeout = 5.minutes
      )

      val owner             = accountGen.generateSample()
      val responses         = ConcurrentSubject.publish[(Channel, PrivateDataResponse)]
      val inventories       = ConcurrentSubject.publish[(Channel, PrivacyInventoryV1)]
      val inventoryRequests = ConcurrentSubject.publish[(Channel, PrivacyInventoryRequest)]
      val privacyInventoryHandlerSettings =
        PrivacyInventoryHandlerSettings(500.millis, PositiveInt(500), PositiveInt(100), 5.minutes, PositiveInt(10))
      val privacyInventoryHandler =
        new PrivacyInventoryHandler(inventories, inventoryRequests, owner, peers, state, storage, privacyInventoryHandlerSettings)

      val synchronizer =
        new EnablePolicyDataSynchronizer(
          state = state,
          owner = owner,
          settings = settings,
          responses = responses,
          privacyInventoryHandler = privacyInventoryHandler,
          peers = peers,
          maxSimultaneousConnections = 30,
          storage = storage,
          strictDataCache = cache,
          time = new TestTime()
        )(scheduler) {
          override protected def selectPeers(policyRecipients: Set[Address], policyId: ByteStr): Vector[PeerSession] =
            Vector(mock[PeerSession])

          override protected def sendToPeers(request: Object, peers: PeerSession*): Task[Unit] =
            Task.unit
        }

      val fibers = synchronizer.internalRun()

      updateState(state, policyUpdates, additionalDataHashTxs)

      val allDataHashTxs = initPendingDataHashTxs ++ additionalDataHashTxs

      def markCondition: Boolean = {
        val allPending = state.pendingPrivacyItems()
        val allLost    = state.lostPrivacyItems()

        allDataHashTxs.forall {
          case PolicyDataWithTxV1(_, tx) =>
            val key = PolicyDataId(tx.policyId, tx.dataHash)
            !allPending.contains(key) && allLost.contains(key)
        }
      }

      waitUntil(markCondition, "Failed to wait until all items marked as lost")

      def startProcessingCondition: Boolean = {
        allDataHashTxs.forall {
          case PolicyDataWithTxV1(_, tx) =>
            fibers.keySet.contains(PolicyDataId(tx.policyId, tx.dataHash))
        }
      }

      waitUntil(startProcessingCondition, "Failed to wait until start processing lost items")
  }

  "Force sync" in fixture {
    case FixtureParams(state, storage, policyId, initDataHashTxs, additionalDataHashTxs, policyUpdates, _, peers, _, scheduler) =>
      implicit val implicitScheduler: Scheduler = scheduler

      val settings = new PrivacySynchronizerSettings(
        requestTimeout = 1.second,
        initRetryDelay = 1.second,
        inventoryStreamTimeout = 1.seconds,
        inventoryRequestDelay = 3.seconds,
        inventoryTimestampThreshold = 10.minutes,
        crawlingParallelism = PositiveInt(100),
        maxAttemptCount = PositiveInt(10),
        lostDataProcessingDelay = 1.minute,
        networkStreamBufferSize = PositiveInt(3),
        failedPeersCacheSize = 100,
        failedPeersCacheExpireTimeout = 5.minutes
      )
      val owner             = accountGen.generateSample()
      val responses         = ConcurrentSubject.publish[(Channel, PrivateDataResponse)]
      val inventories       = ConcurrentSubject.publish[(Channel, PrivacyInventoryV1)]
      val inventoryRequests = ConcurrentSubject.publish[(Channel, PrivacyInventoryRequest)]
      val privacyInventoryHandlerSettings =
        PrivacyInventoryHandlerSettings(500.millis, PositiveInt(500), PositiveInt(100), 5.minutes, PositiveInt(10))
      val privacyInventoryHandler =
        new PrivacyInventoryHandler(inventories, inventoryRequests, owner, peers, state, storage, privacyInventoryHandlerSettings)
      val synchronizer =
        new EnablePolicyDataSynchronizer(
          state = state,
          owner = owner,
          settings = settings,
          responses = responses,
          privacyInventoryHandler = privacyInventoryHandler,
          peers = peers,
          maxSimultaneousConnections = 30,
          storage = storage,
          strictDataCache = cache,
          time = new TestTime()
        )(scheduler)

      val fibers = synchronizer.internalRun()
      waitUntil(fibers.size == initDataHashTxs.size, "Failed to wait until all init crawling tasks started")

      initDataHashTxs.foreach {
        case PolicyDataWithTxV1(_, tx) =>
          fibers.keySet should contain(PolicyDataId(policyId, tx.dataHash))
      }

      updateState(state, policyUpdates, additionalDataHashTxs)

      additionalDataHashTxs.foreach {
        case PolicyDataWithTxV1(_, tx) =>
          val key = PolicyDataId(policyId, tx.dataHash)
          state.pendingPrivacyItems() should contain(key)
          fibers.keySet should contain(key)
      }

      val allDataHashTxs = initDataHashTxs ++ additionalDataHashTxs

      val (newFibers, count) = synchronizer.internalForceSync()
      count shouldBe allDataHashTxs.size
      waitUntil(fibers.isEmpty, "Failed to wait until all crawling tasks stopped")
      waitUntil(newFibers.size == allDataHashTxs.size, "Failed to wait until all crawling tasks restarted")

      allDataHashTxs.foreach {
        case PolicyDataWithTxV1(_, tx) =>
          val key = PolicyDataId(policyId, tx.dataHash)
          state.pendingPrivacyItems() should contain(key)
          newFibers.keySet should contain(key)
      }
  }

  "Force sync by policy id" in fixture {
    case FixtureParams(state, storage, policyId, initDataHashTxs, additionalDataHashTxs, policyUpdates, _, peers, _, scheduler) =>
      implicit val implicitScheduler: Scheduler = scheduler

      val settings = new PrivacySynchronizerSettings(
        requestTimeout = 1.second,
        initRetryDelay = 1.second,
        inventoryStreamTimeout = 1.seconds,
        inventoryRequestDelay = 3.seconds,
        inventoryTimestampThreshold = 10.minutes,
        crawlingParallelism = PositiveInt(100),
        maxAttemptCount = PositiveInt(20),
        lostDataProcessingDelay = 1.minute,
        networkStreamBufferSize = PositiveInt(3),
        failedPeersCacheSize = 100,
        failedPeersCacheExpireTimeout = 5.minutes
      )
      val owner             = accountGen.generateSample()
      val responses         = ConcurrentSubject.publish[(Channel, PrivateDataResponse)]
      val inventories       = ConcurrentSubject.publish[(Channel, PrivacyInventoryV1)]
      val inventoryRequests = ConcurrentSubject.publish[(Channel, PrivacyInventoryRequest)]
      val privacyInventoryHandlerSettings =
        PrivacyInventoryHandlerSettings(500.millis, PositiveInt(500), PositiveInt(100), 5.minutes, PositiveInt(10))
      val privacyInventoryHandler =
        new PrivacyInventoryHandler(inventories, inventoryRequests, owner, peers, state, storage, privacyInventoryHandlerSettings)
      val synchronizer =
        new EnablePolicyDataSynchronizer(
          state = state,
          owner = owner,
          settings = settings,
          responses = responses,
          privacyInventoryHandler = privacyInventoryHandler,
          peers = peers,
          maxSimultaneousConnections = 30,
          storage = storage,
          strictDataCache = cache,
          time = new TestTime()
        )(scheduler)

      val missedDataHashTx = additionalDataHashTxs.head
      val otherDataHashTx  = additionalDataHashTxs.tail

      (state.policyDataHashes _).expects(policyId).returning(Set(missedDataHashTx.tx.dataHash)).anyNumberOfTimes()

      val fibers = synchronizer.internalRun()
      waitUntil(fibers.size == initDataHashTxs.size, "Failed to wait until all init crawling tasks started")

      initDataHashTxs.foreach {
        case PolicyDataWithTxV1(_, tx) =>
          fibers.keySet should contain(PolicyDataId(policyId, tx.dataHash))
      }

      updateState(state, policyUpdates, otherDataHashTx)

      otherDataHashTx.foreach {
        case PolicyDataWithTxV1(_, tx) =>
          val key = PolicyDataId(policyId, tx.dataHash)
          state.pendingPrivacyItems() should contain(key)
          fibers.keySet should contain(key)
      }

      val allDataHashTxs = missedDataHashTx :: initDataHashTxs ++ otherDataHashTx

      val (newFibers, count) = synchronizer.internalForceSync(policyId)
      count shouldBe allDataHashTxs.size
      waitUntil(fibers.isEmpty, "Failed to wait until all crawling tasks stopped")
      waitUntil(newFibers.size == allDataHashTxs.size, "Failed to wait until all crawling tasks restarted")

      allDataHashTxs.foreach {
        case PolicyDataWithTxV1(_, tx) =>
          val key = PolicyDataId(policyId, tx.dataHash)
          state.pendingPrivacyItems() should contain(key)
          newFibers.keySet should contain(key)
      }
  }

  private def updateState(
      state: TestState,
      policyUpdates: ConcurrentSubject[PolicyUpdate, PolicyUpdate],
      dataWithTxs: List[PolicyDataWithTxV1],
      txIdToTs: Map[ByteStr, Long] = Map.empty
  )(implicit scheduler: Scheduler): Unit = {
    processDataWithTxs(dataWithTxs) {
      case PolicyDataWithTxV1(_, tx) =>
        state.addToPending(tx.policyId, tx.dataHash)
        policyUpdates.onNext(PolicyUpdate(PolicyDataId(tx.policyId, tx.dataHash), txIdToTs.get(tx.id())))
    }
  }

  private def rollbackState(
      state: TestState,
      policyRollbacks: ConcurrentSubject[PolicyDataId, PolicyDataId],
      dataWithTxs: List[PolicyDataWithTxV1]
  )(implicit scheduler: Scheduler): Unit = {
    processDataWithTxs(dataWithTxs) {
      case PolicyDataWithTxV1(_, tx) =>
        state.removeFromPendingAndLost(tx.policyId, tx.dataHash)
        policyRollbacks.onNext(PolicyDataId(tx.policyId, tx.dataHash))
    }
  }

  private def processDataWithTxs(
      dataWithTxs: List[PolicyDataWithTxV1]
  )(f: PolicyDataWithTxV1 => Future[Ack])(implicit scheduler: Scheduler): Unit =
    dataWithTxs
      .map(dataWithTx => Task.deferFuture(f(dataWithTx)))
      .sequence_
      .delayResult(100.millis)
      .runToFuture
      .futureValue

  private def waitUntil(condition: => Boolean, failMessage: String, delayBetweenSteps: Int = 500, retriesCount: Int = 30): Unit = {
    @tailrec
    def insideRecursion(retryRemains: Int): Unit = {
      val conditionResult = condition
      if (!conditionResult) {
        if (retryRemains > 0) {
          Thread.sleep(delayBetweenSteps)
          insideRecursion(retryRemains - 1)
        } else {
          fail(failMessage)
        }
      }
    }

    insideRecursion(retriesCount)
  }

  private def buildTestPolicyState(policyTx: CreatePolicyTransaction,
                                   initDataHashTxs: List[PolicyDataWithTxV1],
                                   policyUpdates: ConcurrentSubject[PolicyUpdate, PolicyUpdate],
                                   policyRollbacks: ConcurrentSubject[PolicyDataId, PolicyDataId],
                                   recipientAccount: PrivateKeyAccount): TestState = {
    val stateMock = mock[TestState]

    (stateMock.policyRecipients(_: ByteStr)).expects(policyTx.id()).returns(policyTx.recipients.toSet + recipientAccount.toAddress).anyNumberOfTimes()
    (stateMock.policyUpdates _).expects().returns(policyUpdates).anyNumberOfTimes()
    (stateMock.policyRollbacks _).expects().returns(policyRollbacks).anyNumberOfTimes()

    val internalPending = ConcurrentHashMap.newKeySet[PolicyDataId]().asScala
    val internalLost    = ConcurrentHashMap.newKeySet[PolicyDataId]().asScala

    (stateMock.pendingPrivacyItems _)
      .expects()
      .onCall(_ => internalPending.toSet)
      .anyNumberOfTimes()

    (stateMock.lostPrivacyItems _)
      .expects()
      .onCall(_ => internalLost.toSet)
      .anyNumberOfTimes()

    (stateMock
      .addToPending(_: ByteStr, _: PolicyDataHash))
      .expects(*, *)
      .onCall((policyId, dataHash) => internalPending.add(PolicyDataId(policyId, dataHash)))
      .anyNumberOfTimes()

    (stateMock
      .addToPending(_: Set[PolicyDataId]))
      .expects(*)
      .onCall { pdi: Set[PolicyDataId] =>
        val oldSize = internalPending.size
        internalPending ++= pdi
        internalPending.size - oldSize
      }
      .anyNumberOfTimes()

    (stateMock.pendingToLost _)
      .expects(*, *)
      .onCall { (policyId, dataHash) =>
        val key = PolicyDataId(policyId, dataHash)
        internalPending.remove(key) ->
          internalLost.add(key)
      }
      .anyNumberOfTimes()

    (stateMock.removeFromPendingAndLost _)
      .expects(*, *)
      .onCall { (policyId, dataHash) =>
        val key = PolicyDataId(policyId, dataHash)
        internalPending.remove(key) ->
          internalLost.remove(key)
      }
      .anyNumberOfTimes()

    (stateMock.putItemDescriptor _).expects(*, *, *).returning(()).anyNumberOfTimes()
    (stateMock.activatedFeatures _)
      .expects()
      .returning(Map[Short, Int](BlockchainFeature.PrivacyLargeObjectSupport.id -> 0))
      .anyNumberOfTimes()
    (stateMock.height _)
      .expects()
      .returning(1)
      .anyNumberOfTimes()

    val initPolicyDataIds = initDataHashTxs.map(pdh => PolicyDataId(pdh.tx.policyId, pdh.tx.dataHash)).toSet
    stateMock.addToPending(initPolicyDataIds)

    stateMock
  }

}
