package com.wavesenterprise.docker

import com.wavesenterprise.RxSetup
import com.wavesenterprise.network.NetworkContractExecutionMessage
import com.wavesenterprise.network.peers.ActivePeerConnections
import com.wavesenterprise.settings.PositiveInt
import com.wavesenterprise.settings.dockerengine.ContractExecutionMessagesCacheSettings
import com.wavesenterprise.transaction.Transaction
import com.wavesenterprise.transaction.docker.CreateContractTransactionV1
import com.wavesenterprise.utils.EitherUtils.EitherExt
import com.wavesenterprise.utils.ResourceUtils
import com.wavesenterprise.utx.UtxPool
import io.netty.channel.Channel
import monix.reactive.subjects.ConcurrentSubject
import org.scalacheck.Gen
import org.scalamock.scalatest.MockFactory
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks
import tools.GenHelper._

import java.util.concurrent.ConcurrentHashMap
import scala.collection.JavaConverters._
import scala.concurrent.Await
import scala.concurrent.duration._
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers

class ContractExecutionMessagesCacheSuite
    extends AnyFreeSpec
    with Matchers
    with MockFactory
    with ScalaCheckPropertyChecks
    with ContractExecutionMessageGen
    with RxSetup {

  private[this] val activePeerConnections = stub[ActivePeerConnections]
  private[this] val channel               = stub[Channel]

  private val ErrorQuorum = 3

  def buildCache(utx: UtxPool = stub[UtxPool], clenupInterval: FiniteDuration = 5.minutes): ContractExecutionMessagesCache =
    new ContractExecutionMessagesCache(
      settings = ContractExecutionMessagesCacheSettings(5.second, 100000, 1, 500.millis, clenupInterval, PositiveInt(ErrorQuorum)),
      dockerMiningEnabled = true,
      activePeerConnections = activePeerConnections,
      utx = utx,
      scheduler = rxScheduler
    )

  "ContractExecutionMessagesCacheSuite" - {
    "test put and get the same message" in {
      val message = messageGen().generateSample()
      val cache   = buildCache()
      cache.put(message.txId, message)
      cache.get(message.txId) shouldBe Some(Set(message))
    }

    "test put message through observable" in {
      ResourceUtils.withResource(buildCache()) { cache =>
        val observable = ConcurrentSubject.publish[(Channel, NetworkContractExecutionMessage)](monix.execution.Scheduler.global)
        cache.subscribe(observable)
        val message = messageGen().generateSample()
        observable.onNext((channel, NetworkContractExecutionMessage(message)))
        Thread.sleep(3.second.toMillis)
        observable.onComplete()
        cache.get(message.txId) shouldBe Some(Set(message))
      }
    }

    "utx cleanup test" in {
      val utx = mock[UtxPool]
      val txWithMessageGen =
        for {
          messagesCount <- Gen.chooseNum(ErrorQuorum - 1, ErrorQuorum + 1)
          txWithMessages <- Gen.listOfN(messagesCount, accountGen).map { accounts =>
            val tx = CreateContractTransactionV1.selfSigned(accounts.head, "foo", Array.fill(64)('a').mkString, "baz", List.empty, 1, 1).explicitGet()
            val messages = accounts.map { acc =>
              ContractExecutionMessage(acc, tx.id(), ContractExecutionStatus.Error, None, "", 1)
            }

            tx -> messages
          }
        } yield txWithMessages

      val removedTxs = ConcurrentHashMap.newKeySet[Transaction]().asScala

      val txWithMessages = Gen.nonEmptyListOf(txWithMessageGen).generateSample()

      (utx.selectTransactions _).expects(*).returns(txWithMessages.map { case (tx, _) => tx }.toArray).atLeastOnce()
      (utx.remove _)
        .expects(*, *, *)
        .onCall { (tx: Transaction, _: Option[_], _: Boolean) =>
          removedTxs.add(tx)
        }
        .atLeastOnce()

      ResourceUtils.withResource(buildCache(utx, 1.second)) { cache =>
        val observable = ConcurrentSubject.publish[(Channel, NetworkContractExecutionMessage)](monix.execution.Scheduler.global)
        cache.subscribe(observable)
        txWithMessages.foreach {
          case (_, messages) =>
            messages.foreach { message =>
              observable.onNext((channel, NetworkContractExecutionMessage(message)))
            }
        }

        Thread.sleep(5.second.toMillis)
        observable.onComplete()
        val expected = txWithMessages.collect { case (tx, messages) if messages.size >= ErrorQuorum => tx }.toSet

        removedTxs.toSet shouldBe expected
      }
    }

    "test expire message" in {
      ResourceUtils.withResource(buildCache()) { cache =>
        val message = messageGen().generateSample()
        cache.put(message.txId, message)
        Thread.sleep(7.second.toMillis)
        cache.get(message.txId) shouldBe None
      }
    }

    "test lastMessage observable" in {
      ResourceUtils.withResource(buildCache()) { cache =>
        val incomingMessages = ConcurrentSubject.publish[(Channel, NetworkContractExecutionMessage)](rxScheduler)
        cache.subscribe(incomingMessages)
        val completionTrigger  = ConcurrentSubject.publish[Unit](rxScheduler)
        val lastMessagesStream = cache.lastMessage.takeUntil(completionTrigger.completed).toListL.executeAsync.runToFuture(rxScheduler)

        val messageA = messageGen().generateSample()
        val messageB = messageGen().generateSample()

        Thread.sleep(2.seconds.toMillis)
        cache.put(messageA.txId, messageA)
        Thread.sleep(2.seconds.toMillis)
        incomingMessages.onNext(channel -> NetworkContractExecutionMessage(messageB))
        Thread.sleep(2.seconds.toMillis)
        completionTrigger.onComplete()
        incomingMessages.onComplete()

        val collectedMessages = Await.result(lastMessagesStream, 10.seconds)
        collectedMessages should contain theSameElementsInOrderAs List(messageA, messageB)
      }
    }
  }
}
