package com.wavesenterprise.docker

import com.google.common.cache.{Cache, CacheBuilder}
import com.wavesenterprise.account.Address
import com.wavesenterprise.crypto
import com.wavesenterprise.network.peers.ActivePeerConnections
import com.wavesenterprise.network.{ChannelObservable, NetworkContractExecutionMessage}
import com.wavesenterprise.settings.dockerengine.ContractExecutionMessagesCacheSettings
import com.wavesenterprise.state.ByteStr
import com.wavesenterprise.transaction.{AtomicTransaction, Transaction}
import com.wavesenterprise.transaction.docker.ExecutableTransaction
import com.wavesenterprise.utils.ScorexLogging
import com.wavesenterprise.utx.UtxPool
import io.netty.channel.Channel
import io.netty.channel.group.ChannelMatcher
import monix.eval.Task
import monix.execution.Scheduler
import monix.reactive.subjects.ConcurrentSubject
import monix.reactive.{Observable, OverflowStrategy}

import java.util.concurrent.TimeUnit

/**
  * Cache for [[ContractExecutionMessage]]
  */
class ContractExecutionMessagesCache(
    settings: ContractExecutionMessagesCacheSettings,
    dockerMiningEnabled: Boolean,
    activePeerConnections: ActivePeerConnections,
    utx: UtxPool,
    implicit val scheduler: Scheduler
) extends AutoCloseable
    with ScorexLogging {

  private[this] val cache: Cache[ByteStr, Set[ContractExecutionMessage]] =
    CacheBuilder.newBuilder().maximumSize(settings.maxSize).expireAfterWrite(settings.expireAfter.toMillis, TimeUnit.MILLISECONDS).build()

  private[this] val internalLastMessage = ConcurrentSubject.publish[ContractExecutionMessage]

  val lastMessage: Observable[ContractExecutionMessage] = internalLastMessage.asyncBoundary(OverflowStrategy.Default)

  private val errorQuorum = settings.contractErrorQuorum.value

  @inline
  def get(txId: ByteStr): Option[Set[ContractExecutionMessage]] = Option(cache.getIfPresent(txId))

  def put(txId: ByteStr, message: ContractExecutionMessage): Unit = {
    saveToCache(txId, message)
    if (message.status != ContractExecutionStatus.Success || !settings.ignoreSuccessful)
      activePeerConnections.broadcast(NetworkContractExecutionMessage(message))
  }

  private[this] val cleanup =
    Observable
      .interval(settings.utxCleanupInterval)
      .mapEval { _ =>
        Task(cleanupUtx())
      }
      .logErr
      .onErrorRestartUnlimited
      .subscribe()

  private def cleanupUtx(): Unit = {
    utx.selectTransactions {
      case _: ExecutableTransaction | _: AtomicTransaction => true
      case _                                               => false
    }.foreach { executableOrAtomicTransaction =>
      errorSendersIfQuorumExceededTransaction(executableOrAtomicTransaction)
        .foreach { errorSendersString =>
          utx.remove(
            executableOrAtomicTransaction,
            Some(s"Received >= '$errorQuorum' statuses with contract execution business error from: '$errorSendersString'"),
            mustBeInPool = true
          )
        }
    }
  }

  def errorSendersIfQuorumExceededTransaction: Transaction => Option[String] = {
    case executableTransaction: ExecutableTransaction => errorSendersIfQuorumExceeded(executableTransaction)
    case atomicTransaction: AtomicTransaction         => errorSendersIfQuorumExceeded(atomicTransaction)
    case _                                            => None
  }

  def errorSendersIfQuorumExceededSeq(atomicTransaction: AtomicTransaction): Seq[String] = {
    atomicTransaction
      .transactions
      .collect {
        case executableTransaction: ExecutableTransaction => executableTransaction
      }
      .flatMap((executableTransaction: ExecutableTransaction) => errorSendersIfQuorumExceeded(executableTransaction))
  }

  def errorSendersIfQuorumExceeded(atomicTransaction: AtomicTransaction): Option[String] = {
    val errorSenders: Seq[String] = errorSendersIfQuorumExceededSeq(atomicTransaction)
    if (errorSenders.nonEmpty) {
      Option(errorSenders.mkString("; "))
    } else { None }
  }

  def errorSendersIfQuorumExceeded(executableTransaction: ExecutableTransaction): Option[String] =
    get(executableTransaction.id())
      .map { contractExecutionMessages =>
        contractExecutionMessages.view
          .filter(message => message.status == ContractExecutionStatus.Error)
          .map(message => Address.fromPublicKey(message.rawSenderPubKey).address)
          .take(errorQuorum)
      }
      .filter(_.size >= errorQuorum)
      .map(_.mkString(", "))

  private[this] def saveToCache(txId: ByteStr, message: ContractExecutionMessage): Set[ContractExecutionMessage] = {
    internalLastMessage.onNext(message)
    cache.asMap().compute(txId, (_, oldValue) => Option(oldValue).map(_ + message).getOrElse(Set(message)))
  }

  def subscribe(observable: ChannelObservable[NetworkContractExecutionMessage]): Unit = {
    observable
      .observeOn(scheduler)
      .bufferTimedAndCounted(settings.maxBufferTime, settings.maxBufferSize)
      .foreach { messagesBuffer =>
        val toAdd = messagesBuffer.filter {
          case (_, NetworkContractExecutionMessage(message)) =>
            val isNew = get(message.txId).forall(!_.contains(message))
            isNew && verifySignature(message)
        }

        if (toAdd.nonEmpty) {
          toAdd
            .groupBy { case (channel, _) => channel }
            .foreach {
              case (sender, xs) => handleMessages(sender, xs)
            }
          activePeerConnections.flushWrites()
        }
      }
  }

  private def handleMessages(sender: Channel, messages: Seq[(Channel, NetworkContractExecutionMessage)]): Unit = {
    val channelMatcher: ChannelMatcher = { (_: Channel) != sender }
    messages.foreach {
      case (_, networkMessage @ NetworkContractExecutionMessage(message)) =>
        saveToCache(message.txId, message)
        if (message.status != ContractExecutionStatus.Success || !settings.ignoreSuccessful)
          activePeerConnections.writeMsg(networkMessage, channelMatcher)

        if (!dockerMiningEnabled && message.status != ContractExecutionStatus.Success) {
          utx
            .transactionById(message.txId)
            .foreach(tx => utx.removeAll(Map(tx -> "Docker mining is disabled and contract wasn't successfully executed")))
        }
    }
  }

  private def verifySignature(message: ContractExecutionMessage): Boolean = {
    val verified = crypto.verify(message.signature.arr, message.bodyBytes(), message.rawSenderPubKey)
    if (!verified) {
      log.warn(s"Can't validate signature for message '${message.json()}'")
    }
    verified
  }

  override def close(): Unit = {
    cleanup.cancel()
    internalLastMessage.onComplete()
  }
}
