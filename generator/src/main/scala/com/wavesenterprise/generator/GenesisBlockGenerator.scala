package com.wavesenterprise.generator

import com.wavesenterprise.account.{AddressSchemeHelper, PrivateKeyAccount}
import com.wavesenterprise.block.{Block, GenesisData}
import com.wavesenterprise.consensus.{CftLikeConsensusBlockData, PoALikeConsensusBlockData, PoSLikeConsensusBlockData}
import com.wavesenterprise.crypto
import com.wavesenterprise.settings._
import com.wavesenterprise.state.ByteStr
import com.wavesenterprise.transaction.ValidationError
import monix.eval.Task

object GenesisBlockGenerator extends BaseGenerator[Unit] with GenesisBlockGeneratorBase {
  override def exceptionHandlers: PartialFunction[Throwable, Unit] =
    PartialFunction.empty

  def generateFlow(args: Array[String]): Task[Unit] =
    Task {
      for {
        configWithFileAndCrypto <- readConfig(args)

        ConfigHolder(_, loadedConfig, configFile) = configWithFileAndCrypto
        _                                         = AddressSchemeHelper.setAddressSchemaByte(loadedConfig)
        currentTime                               = time.getTimestamp()

        blockchainSettings  <- getBlockchainSettings(loadedConfig)
        signerWithSignature <- getGenesisInfo(blockchainSettings, currentTime)
        _                   <- writeGenesis(configFile, signerWithSignature)
      } yield ()
    }.flatMap {
      case Left(err) => Task.raiseError(new RuntimeException(err.toString))
      case Right(_)  => Task.unit
    }

  private[generator] def getGenesisInfo(
      blockchainSettings: BlockchainSettings,
      currentTime: Long
  ): Either[ValidationError, GenesisUpdates] = {
    blockchainSettings.custom.genesis.toPlainSettings.map(_.copy(blockTimestamp = currentTime)).flatMap(validateGenesisSettings).flatMap {
      genesisSettings =>
        val txs       = Block.genesisTransactions(genesisSettings)
        val reference = ByteStr(Array.fill(crypto.SignatureLength)(-1: Byte))
        val consensusGenesisData = blockchainSettings.consensus.consensusType match {
          case ConsensusType.PoS =>
            PoSLikeConsensusBlockData(genesisSettings.initialBaseTarget, ByteStr(Array.fill(crypto.DigestSize)(0: Byte)))
          case ConsensusType.PoA =>
            PoALikeConsensusBlockData(overallSkippedRounds = 0)
          case ConsensusType.CFT =>
            CftLikeConsensusBlockData(votes = Seq.empty, overallSkippedRounds = 0)
        }

        for {
          signerKeypair <- Right(crypto.generateKeyPair())

          signerAccount = PrivateKeyAccount(signerKeypair)
          blockVersion  = Block.selectGenesisBlockVersion(genesisSettings)
          genesisData   = GenesisData(genesisSettings.senderRoleEnabled)

          genesisUpdates <- Block
            .buildAndSign(
              version = blockVersion,
              timestamp = genesisSettings.blockTimestamp,
              reference = reference,
              consensusData = consensusGenesisData,
              transactionData = txs,
              signer = signerAccount,
              featureVotes = Set.empty,
              genesisDataOpt = Some(genesisData)
            )
            .map { genesisBlock =>
              val signerPkBase58  = signerAccount.publicKeyBase58
              val signatureBase58 = genesisBlock.signerData.signature.base58
              GenesisUpdates(signerPkBase58, signatureBase58, currentTime)
            }
        } yield genesisUpdates
    }
  }

  override def internalClose(): Unit = {
    time.close()
  }
}
