package com.wavesenterprise.network

import akka.actor.ActorSystem
import com.wavesenterprise.account.PrivateKeyAccount
import com.wavesenterprise.database.PrivacyState
import com.wavesenterprise.network.peers.{ActivePeerConnections, PeerConnection, PeerInfo}
import com.wavesenterprise.network.privacy.EnablePolicyDataSynchronizer.StrictResponse
import com.wavesenterprise.network.privacy.{EnablePolicyDataReplier, EnablePolicyDataSynchronizer, PolicyStrictDataCache}
import com.wavesenterprise.privacy.{PolicyDataHash, PolicyStorage, PrivacyDataType, PrivacyItemDescriptor}
import com.wavesenterprise.settings.PositiveInt
import com.wavesenterprise.settings.privacy.{PolicyDataCacheSettings, PrivacyReplierSettings}
import com.wavesenterprise.state.{Blockchain, ByteStr}
import com.wavesenterprise.utils.EitherUtils.EitherExt
import com.wavesenterprise.utils.ScorexLogging
import com.wavesenterprise.{NodeVersion, TransactionGen}
import io.netty.channel.Channel
import io.netty.channel.embedded.EmbeddedChannel
import monix.eval.Task
import monix.execution.Scheduler
import monix.execution.schedulers.SchedulerService
import monix.reactive.subjects.ConcurrentSubject
import org.scalamock.scalatest.MockFactory
import org.scalatest.concurrent.ScalaFutures
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks
import squants.information.Mebibytes
import tools.GenHelper._

import java.net.SocketAddress
import scala.concurrent.Await
import scala.concurrent.duration._
import scala.util.Random
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers

class PolicyDataReplierSpec
    extends AnyFreeSpec
    with Matchers
    with ScalaCheckPropertyChecks
    with ScalaFutures
    with MockFactory
    with TransactionGen
    with ScorexLogging {

  override implicit val patienceConfig: PatienceConfig = PatienceConfig(timeout = 15.seconds, interval = 300.millis)

  case class FixtureParams(
      blockchain: Blockchain with PrivacyState,
      storage: PolicyStorage,
      policyId: ByteStr,
      allDataWithTxs: List[PolicyDataWithTxV1],
      existingDataHashes: Set[PolicyDataHash],
      peers: ActivePeerConnections,
      scheduler: Scheduler,
      cache: PolicyStrictDataCache
  )

  def fixture(test: FixtureParams => Unit): Unit = {
    implicit val scheduler: SchedulerService = Scheduler.singleThread("PolicyDataReplierScheduler")

    val existingDataHashCount = 2
    val (CreatePolicyTransactionV1TestWrap(policyTx, _), allDataWithTx) = createPolicyAndDataHashTransactionsV1Gen(existingDataHashCount + 2)
      .generateSample()

    val existingDataWithTx = allDataWithTx.take(existingDataHashCount)
    val existingDataHashes = existingDataWithTx.map(_.tx.dataHash).toSet

    trait StateType extends Blockchain with PrivacyState
    val blockchain = mock[StateType]
    (blockchain.policyRecipients(_: ByteStr)).expects(policyTx.id()).returns(policyTx.recipients.toSet).anyNumberOfTimes()
    (blockchain.participantPubKey _).expects(*).returns(Some(policyTx.sender)).anyNumberOfTimes()
    (blockchain.privacyItemDescriptor _).expects(*, *).returns(Some(PrivacyItemDescriptor(PrivacyDataType.Default))).anyNumberOfTimes()
    (blockchain.activatedFeatures _).expects().returns(Map.empty).anyNumberOfTimes()

    val storage = TestPolicyStorage.build()

    existingDataWithTx.foreach {
      case PolicyDataWithTxV1(data, tx) =>
        val metaData = policyMetaData.generateSample().copy(policyId = tx.policyId.toString, hash = tx.dataHash.toString)
        Await.result(storage.savePolicyDataWithMeta(Left(ByteStr(data)), metaData).runToFuture, 10.seconds)
    }

    val peers = new ActivePeerConnections(100)

    val cache = new PolicyStrictDataCache(new PolicyDataCacheSettings(100, 10 minutes))

    val fixtureParams = FixtureParams(blockchain, storage, policyTx.id(), allDataWithTx, existingDataHashes, peers, scheduler, cache)

    try {
      test(fixtureParams)
    } finally {
      scheduler.shutdown()
    }
  }

  "Process encrypted data request" in fixture {
    case FixtureParams(blockchain, storage, policyId, allDataWithTxs, existingDataHashes, peers, scheduler, cache) =>
      implicit val implicitScheduler: Scheduler = scheduler
      implicit val actorSys: ActorSystem        = ActorSystem()

      val requests = ConcurrentSubject.publish[(Channel, PrivateDataRequest)]
      val settings = new PrivacyReplierSettings(PositiveInt(100), 1.minute, Mebibytes(3), PositiveInt(66))
      val replier  = new EnablePolicyDataReplier(blockchain, settings, requests, peers, storage, cache)
      replier.run()

      val channel         = new EmbeddedChannel()
      val localSessionKey = PrivateKeyAccount(com.wavesenterprise.crypto.generateSessionKeyPair())
      val peerSessionKey  = PrivateKeyAccount(com.wavesenterprise.crypto.generateSessionKeyPair())
      val peerInfo = PeerInfo(
        remoteAddress = mock[SocketAddress],
        declaredAddress = None,
        chainId = 'I',
        nodeVersion = NodeVersion(1, 0, 0),
        applicationConsensus = "PoS",
        nodeName = "node",
        nodeNonce = 0L,
        nodeOwnerAddress = Random.shuffle(blockchain.policyRecipients(policyId).toSeq).head,
        sessionPubKey = peerSessionKey
      )

      peers.putIfAbsentAndMaxNotReachedOrReplaceValidator(new PeerConnection(channel, peerInfo, localSessionKey))

      allDataWithTxs.foreach {
        case PolicyDataWithTxV1(data, tx) =>
          Task
            .deferFuture(requests.onNext(channel -> PrivateDataRequest(tx.policyId, tx.dataHash)))
            .delayResult(1.second)
            .runToFuture
            .futureValue

          if (existingDataHashes.contains(tx.dataHash)) {
            val response = channel.readOutbound[GotEncryptedDataResponse]()

            val StrictResponse(_, decryptedData) = EnablePolicyDataSynchronizer.StrictResponse
              .decryptStrictResponse(tx.policyId, tx.dataHash, peers, channel, response.encryptedData)
              .runToFuture
              .futureValue

            response.policyId shouldBe policyId
            response.dataHash shouldBe tx.dataHash
            decryptedData.arr shouldBe data
          } else {
            channel.readOutbound[NoDataResponse]() shouldBe NoDataResponse(policyId, tx.dataHash)
          }
      }
  }

  "Process data request" in fixture {
    case FixtureParams(blockchain, storage, policyId, allDataWithTxs, existingDataHashes, peers, scheduler, cache) =>
      implicit val implicitScheduler: Scheduler = scheduler
      implicit val actorSystem                  = ActorSystem()
      val requests                              = ConcurrentSubject.publish[(Channel, PrivateDataRequest)]
      val settings                              = new PrivacyReplierSettings(PositiveInt(100), 1.minute, Mebibytes(3), PositiveInt(66))
      val replier                               = new EnablePolicyDataReplier(blockchain, settings, requests, peers, storage, cache)
      replier.run()

      val channel = new EmbeddedChannel()
      channel.attr[Unit](Attributes.TlsAttribute).set(Unit)
      val localSessionKey = PrivateKeyAccount(com.wavesenterprise.crypto.generateSessionKeyPair())
      val peerSessionKey  = PrivateKeyAccount(com.wavesenterprise.crypto.generateSessionKeyPair())
      val peerInfo = PeerInfo(
        remoteAddress = mock[SocketAddress],
        declaredAddress = None,
        chainId = 'I',
        nodeVersion = NodeVersion(1, 0, 0),
        applicationConsensus = "PoS",
        nodeName = "node",
        nodeNonce = 0L,
        nodeOwnerAddress = Random.shuffle(blockchain.policyRecipients(policyId).toSeq).head,
        sessionPubKey = peerSessionKey
      )

      peers.putIfAbsentAndMaxNotReachedOrReplaceValidator(new PeerConnection(channel, peerInfo, localSessionKey))

      allDataWithTxs.foreach {
        case PolicyDataWithTxV1(data, tx) =>
          Task
            .deferFuture(requests.onNext(channel -> PrivateDataRequest(tx.policyId, tx.dataHash)))
            .delayResult(100.millis)
            .runToFuture
            .futureValue

          if (existingDataHashes.contains(tx.dataHash)) {
            val response = channel.readOutbound[GotDataResponse]()

            val StrictResponse(_, parsedData) = EnablePolicyDataSynchronizer.StrictResponse
              .deserializeMetaDataAndData(response.data.arr)
              .explicitGet()

            response.policyId shouldBe policyId
            response.dataHash shouldBe tx.dataHash
            parsedData.arr shouldBe data
          } else {
            channel.readOutbound[NoDataResponse]() shouldBe NoDataResponse(policyId, tx.dataHash)
          }
      }
  }
}
