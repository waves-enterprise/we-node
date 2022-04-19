package com.wavesenterprise.anchoring

import akka.actor.ActorSystem
import cats.syntax.either._
import com.wavesenterprise.account.PrivateKeyAccount
import com.wavesenterprise.crypto.WavesKeyStore
import com.wavesenterprise.network.TxBroadcaster
import com.wavesenterprise.settings.{AnchoringDisableSettings, AnchoringEnableSettings, AnchoringSettings}
import com.wavesenterprise.state.NG
import com.wavesenterprise.transaction.{BlockchainUpdater, ValidationError}
import com.wavesenterprise.utils.{NTP, ScorexLogging}
import monix.execution.cancelables.SerialCancelable

/**
  * Anchoring wrapper class to instantiate anchoring scheduler.
  * Runs and stops anchoring scheduler.
  */
trait Anchoring {
  def run(): Unit
  def stop(): Unit
  def configuration: AnchoringConfiguration
}

object DisabledAnchoring extends Anchoring {
  override def run(): Unit                           = ()
  override def stop(): Unit                          = ()
  override def configuration: AnchoringConfiguration = AnchoringConfiguration.DisabledAnchoringCfg
}

class EnabledAnchoring(anchoringScheduler: AnchoringScheduler, val tokenProvider: TargetnetAuthTokenProvider) extends Anchoring with ScorexLogging {

  private val process: SerialCancelable              = SerialCancelable()
  private val tokenProviderProcess: SerialCancelable = SerialCancelable()

  def run(): Unit = {
    log.info("Starting anchoring...")
    process := anchoringScheduler.run()
    tokenProviderProcess := tokenProvider.start()
    log.info("Anchoring started")
  }

  def stop(): Unit = {
    if (!process.isCanceled) {
      process.cancel()
    }
    if (!tokenProviderProcess.isCanceled) {
      tokenProviderProcess.cancel()
    }
  }
  override def configuration: AnchoringConfiguration = anchoringScheduler.anchoringCfg
}

object Anchoring extends ScorexLogging {

  def createAnchoring(
      ownerPrivateKey: PrivateKeyAccount,
      settings: AnchoringSettings,
      blockchainUpdater: BlockchainUpdater with NG,
      time: NTP,
      txBroadcaster: TxBroadcaster)(implicit system: ActorSystem, scheduler: monix.execution.Scheduler): Either[Exception, Anchoring] = {
    settings match {
      case enableSettings: AnchoringEnableSettings =>
        val anchoringWalletSettings = enableSettings.targetnet.wallet
        val wavesKeyStore = new WavesKeyStore(
          storageFolder = anchoringWalletSettings.file,
          password = anchoringWalletSettings.password.toCharArray,
          chainId = enableSettings.targetnet.numericSchemeByte
        )

        val targetnetAuthTokenProvider = TargetnetAuthTokenProvider(enableSettings.targetnet.auth)

        val result = for {
          targetnetKeyPair <- wavesKeyStore.getKeyPair(enableSettings.targetnet.nodeRecipientAddress,
                                                       enableSettings.targetnet.privateKeyPassword.map(_.toCharArray)) match {
            case Right(keyPair) => Right(keyPair)
            case Left(err) =>
              val keystorePathString = anchoringWalletSettings.file
                .map(_.getAbsolutePath)
                .getOrElse("no-path-provided")
              log.error(
                s"Anchoring initialization error: failed to get targetnet KeyPair from provided keystore '$keystorePathString'. Error message: '${err.message}'")
              Left(ValidationError.MissingSenderPrivateKey)
          }

          anchoringCfg = AnchoringConfiguration.EnabledAnchoringCfg(
            targetnetAuthTokenProvider,
            enableSettings.targetnet.nodeAddress,
            enableSettings.targetnet.numericSchemeByte,
            ownerPrivateKey,
            enableSettings.targetnet.nodeRecipientAddress,
            targetnetKeyPair.getPublic,
            targetnetKeyPair.getPrivate,
            enableSettings.targetnet.fee,
            enableSettings.sidechainFee,
            enableSettings.heightCondition,
            enableSettings.threshold,
            enableSettings.txMiningCheckDelay,
            enableSettings.txMiningCheckCount
          )

          anchoringScheduler = new AnchoringScheduler(
            anchoringCfg,
            blockchainUpdater,
            time,
            txBroadcaster
          )
        } yield new EnabledAnchoring(anchoringScheduler, targetnetAuthTokenProvider)

        result.leftMap { validationError =>
          log.error(s"Failed to initialize anchoring, cause: $validationError")
          new RuntimeException(validationError.toString)
        }

      case AnchoringDisableSettings =>
        Right(DisabledAnchoring)
    }
  }
}
