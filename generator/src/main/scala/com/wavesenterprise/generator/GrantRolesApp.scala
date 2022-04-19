package com.wavesenterprise.generator

import cats.implicits._
import com.typesafe.config.ConfigFactory
import com.wavesenterprise.account.PrivateKeyAccount
import com.wavesenterprise.crypto.CryptoInitializer
import com.wavesenterprise.generator.config.FicusImplicits
import com.wavesenterprise.network.RawBytes
import com.wavesenterprise.network.client.NetworkSender
import com.wavesenterprise.network.handshake.SignedHandshakeV2
import com.wavesenterprise.network.message.MessageSpec.TransactionSpec
import com.wavesenterprise.settings.WEConfigReaders._
import com.wavesenterprise.settings.{CryptoSettings, FeeSettings, VersionConstants}
import com.wavesenterprise.transaction.ValidationError
import com.wavesenterprise.transaction.acl.PermitTransaction
import com.wavesenterprise.{ApplicationInfo, NodeVersion, OwnerCredentials, Version, crypto, network}
import monix.eval.{Coeval, Task}
import net.ceedubs.ficus.readers.ArbitraryTypeReader._
import net.ceedubs.ficus.readers.{NameMapper, ValueReader}
import pureconfig.ConfigSource

import java.io.File
import java.net.InetSocketAddress
import scala.util.control.NonFatal

/**
  * Idea is delirious about this import, don't remove it
  * In case of compile error:
  *   import com.wavesenterprise.settings.inetSocketAddressReader
  */
import scala.util.Random

case class GrantRolesSettings(chainId: String,
                              consensusType: String,
                              wavesCrypto: Boolean,
                              ownerCredentials: OwnerCredentials,
                              account: AccountSettings,
                              sendTo: Seq[InetSocketAddress],
                              grants: Seq[GrantSettings],
                              txsPerBucket: Int) {
  val addressScheme: Char = chainId.head

  val cryptoSettings: CryptoSettings = CryptoSettings.WavesCryptoSettings
}

object GrantRolesApp extends BaseGenerator[Seq[Seq[String]]] with FicusImplicits {
  type MaybeOrErr[A] = Either[ValidationError, A]

  implicit val readConfigInHyphen: NameMapper                          = net.ceedubs.ficus.readers.namemappers.implicits.hyphenCase // IDEA bug
  implicit val inetSocketAddressReader: ValueReader[InetSocketAddress] = com.wavesenterprise.settings.inetSocketAddressReader

  override def exceptionHandlers: PartialFunction[Throwable, Unit] = PartialFunction.empty

  def generateFlow(args: Array[String]): Task[Seq[Seq[String]]] = {
    val configPath   = args.headOption.fold(exitWithError("Configuration file path not specified!"))(identity)
    val parsedConfig = ConfigFactory.parseFile(new File(configPath))
    val config       = ConfigFactory.load(parsedConfig).as[GrantRolesSettings]("permission-granter")
    val fees         = ConfigSource.fromConfig(parsedConfig).at("generator.fees").loadOrThrow[FeeSettings.FeesEnabled]
    CryptoInitializer.init(config.cryptoSettings).left.foreach(error => exitWithError(error.message))

    val chainId  = config.addressScheme
    val nodeName = "permission-granter"

    val handshakeMaker: Coeval[SignedHandshakeV2] = {
      val ownerAddress  = config.ownerCredentials.ownerAddress
      val ownerPassword = config.ownerCredentials.ownerPassword
      val privateKeyOpt = config.account.getKeyWithPassword(ownerAddress, Some(ownerPassword))

      if (privateKeyOpt.isEmpty) {
        log.error(s"Failed to get ownerKey from wallet by address ${ownerAddress.stringRepr}")
        close(1)
      }
      val ownerKey = privateKeyOpt.get
      Coeval.eval {
        val sessionKey = PrivateKeyAccount(crypto.generateSessionKeyPair())
        SignedHandshakeV2.createAndSign(
          ApplicationInfo(VersionConstants.ApplicationName + chainId,
                          NodeVersion(Version.VersionTuple),
                          config.consensusType,
                          nodeName,
                          Random.nextLong(),
                          None),
          sessionKey,
          ownerKey
        )
      }
    }
    import scala.concurrent.ExecutionContext.Implicits.global
    val sender = new NetworkSender(network.TrafficLogger.IgnoreNothing, handshakeMaker)
    sys.addShutdownHook(sender.close())

    val senderAcc   = config.account.accounts.head
    val permitTxFee = fees.forTxType(PermitTransaction.typeId)

    val transactionsTask =
      config.grants.toList
        .traverse[MaybeOrErr, List[PermitTransaction]](_.mkTransactions(senderAcc, permitTxFee))
        .map(_.flatten)
        .fold(err => Task.raiseError(new Exception(s"Error during transaction building: $err")), Task.now)

    val nodes: Seq[InetSocketAddress] = config.sendTo

    val result = transactionsTask
      .flatMap { transactions =>
        val bucketized = transactions.sliding(config.txsPerBucket, config.txsPerBucket).toSeq

        Task
          .parTraverse(bucketized) { txsBucket =>
            // serialize transactions for sending
            // connect to every node and send bucket of transactions over network
            val txsMessages = txsBucket.map(tx => RawBytes(TransactionSpec.messageCode, tx.bytes()))
            sendToNodes(sender, nodes, txsMessages)
          }
          .onErrorRecoverWith {
            case NonFatal(ex) =>
              log.error(s"Failed with exception: ${ex.getMessage}")
              Task.raiseError(ex)
          }
      }
      .map { results =>
        log.info("Success!")
        log.info(results.flatten.mkString("\n"))
        results
      }

    result
  }

  def sendToNodes(sender: NetworkSender, nodes: Seq[InetSocketAddress], messages: Seq[RawBytes]): Task[Seq[String]] = {
    val txsCountToSend = messages.size
    Task.parTraverse(nodes) { node =>
      val sendingTask = for {
        channel <- Task.deferFuture(sender.connect(node))
        _       <- Task.deferFuture(sender.send(channel, messages: _*))
      } yield s"Sent $txsCountToSend txs to node ${node.getAddress.toString}"

      sendingTask.onErrorRecoverWith {
        case NonFatal(ex) =>
          log.error(s"Sending failed", ex)
          Task.raiseError(ex)
      }
    }
  }

  override def internalClose(): Unit = {}
}
