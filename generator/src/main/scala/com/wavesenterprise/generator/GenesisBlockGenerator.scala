package com.wavesenterprise.generator

import com.typesafe.config.Config
import com.wavesenterprise.account.{AddressSchemeHelper, PrivateKeyAccount}
import com.wavesenterprise.acl.Role
import com.wavesenterprise.block.{Block, GenesisData}
import com.wavesenterprise.consensus.{CftLikeConsensusBlockData, PoALikeConsensusBlockData, PoSLikeConsensusBlockData}
import com.wavesenterprise.crypto
import com.wavesenterprise.crypto.CryptoInitializer
import com.wavesenterprise.settings._
import com.wavesenterprise.state.ByteStr
import com.wavesenterprise.transaction.ValidationError
import com.wavesenterprise.utils.NTP
import monix.eval.Task
import pureconfig.ConfigSource
import pureconfig.error.ConfigReaderException

import java.io.{File, PrintWriter}
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import scala.collection.mutable.ListBuffer
import scala.io.Source
import scala.util.{Failure, Success, Try}

object GenesisBlockGenerator extends BaseGenerator[Unit] {
  private val timeFormatter  = DateTimeFormatter.ofPattern("dd-MM-yyyy.HH-mm-ss")
  private val currentTimeStr = timeFormatter.format(LocalDateTime.now())
  private val time           = new NTP(Seq("pool.ntp.org"))(monix.execution.Scheduler.global)

  override def exceptionHandlers: PartialFunction[Throwable, Unit] = PartialFunction.empty

  def generateFlow(args: Array[String]): Task[Unit] =
    Task {
      for {
        configWithFile <- readConfig(args)
        (loadedConfig, configFile) = configWithFile
        _                          = AddressSchemeHelper.setAddressSchemaByte(loadedConfig)
        blockchainSettings  <- getBlockchainSettings(loadedConfig)
        signerWithSignature <- getGenesisInfo(blockchainSettings)
        _                   <- writeGenesis(configFile, signerWithSignature)
      } yield ()
    }.flatMap {
      case Left(err) => Task.raiseError(new RuntimeException(err.toString))
      case Right(_)  => Task.unit
    }

  private def getBlockchainSettings(config: Config): Either[ValidationError, BlockchainSettings] = {
    ConfigSource
      .fromConfig(config)
      .at(BlockchainSettings.configPath)
      .load[BlockchainSettings]
      .fold(
        err => Left(ValidationError.GenericError(s"${ConfigReaderException[BlockchainSettings](err).getMessage()}")),
        Right(_)
      )
  }

  private[generator] def getGenesisInfo(blockchainSettings: BlockchainSettings): Either[ValidationError, GenesisUpdates] = {
    val currentTime = time.getTimestamp()
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

        val signerKeypair = crypto.generateKeyPair()
        val signerAccount = PrivateKeyAccount(signerKeypair)
        val blockVersion  = Block.selectGenesisBlockVersion(genesisSettings)
        val genesisData   = GenesisData(genesisSettings.senderRoleEnabled)

        Block
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
    }
  }

  private def validateGenesisSettings(settings: PlainGenesisSettings): Either[ValidationError, PlainGenesisSettings] = {
    val validationErrors = ListBuffer.empty[String]

    val duplicatedParticipants = settings.networkParticipants
      .groupBy(_.publicKey)
      .mapValues(_.size)
      .collect {
        case (participant, count) if count > 1 => participant
      }
      .toList
    if (duplicatedParticipants.nonEmpty) {
      validationErrors += s"The following network participants are duplicated: '${duplicatedParticipants.mkString("[", ",", "]")}'"
    }

    if (!settings.networkParticipants.exists(_.roles.contains(Role.Permissioner.prefixS))) {
      validationErrors += s"At least one network participant must have the '${Role.Permissioner.prefixS}' role"
    }

    if (settings.senderRoleEnabled) {
      if (settings.version.value < 2) {
        validationErrors += "'sender-role-enabled' config param can't be used with Genesis version below '2'"
      }
      settings.networkParticipants
        .find { participant =>
          participant.roles.nonEmpty && !participant.roles.contains(Role.Sender.prefixS)
        }
        .foreach(_ => validationErrors += s"Role '${Role.Sender.prefixS}' is required for every network participant which has any additional role")

    }

    if (settings.initialBalance.units != settings.transactions.map(_.amount.units).sum) {
      validationErrors += "The value of 'initial-balance' has to be equal to the sum of transactions amounts"
    }

    Either.cond(validationErrors.isEmpty, settings, ValidationError.GenericError(validationErrors.mkString("[", ",", "]")))
  }

  private def readConfig(args: Array[String]): Either[ValidationError, (Config, File)] = {
    for {
      configPath <- args.headOption.map(Right(_)).getOrElse(Left(ValidationError.GenericError("Specify path to WE configuration file")))
      configFile <- {
        val maybeFile = new File(configPath)
        Either.cond(maybeFile.exists(), maybeFile, ValidationError.GenericError(s"Configuration file '$configPath' does not exist!"))
      }
      configSource <- Right(buildSourceBasedOnDefault(ConfigSource.file(configFile)))
      loadedConfig <- configSource
        .value()
        .fold(
          err => Left(ValidationError.GenericError(s"${ConfigReaderException[WESettings](err).getMessage()}")),
          config => Right(config.toConfig)
        )
      cryptoCfg <- buildSourceBasedOnDefault(ConfigSource.file(configFile))
        .at(WESettings.configPath)
        .load[CryptoSettings]
        .fold(
          err => Left(ValidationError.GenericError(s"${ConfigReaderException[CryptoSettings](err).getMessage()}")),
          Right(_)
        )
      _ <- CryptoInitializer.init(cryptoCfg).left.map(ValidationError.fromCryptoError)
    } yield (loadedConfig, configFile)
  }

  private def writeGenesis(configFile: File, genesisUpdates: GenesisUpdates): Either[ValidationError, Unit] = {
    for {
      oldConfigFile <- {
        val file = new File(configFile.getAbsolutePath + s".old.$currentTimeStr")
        Either.cond(
          !file.exists(),
          file,
          ValidationError.GenericError(s"Cannot rename ${configFile.getAbsolutePath} to ${file.getAbsolutePath}: File already exists")
        )
      }
      _ <- Either.cond(
        configFile.renameTo(oldConfigFile),
        (),
        ValidationError.GenericError(s"Couldn't rename ${configFile.getAbsolutePath} to ${oldConfigFile.getAbsolutePath}")
      )
      _ <- Either.cond(
        configFile.createNewFile(),
        (),
        ValidationError.GenericError(s"Couldn't create file ${configFile.getAbsolutePath} after renaming")
      )
      _ <- writeToFile(configFile, oldConfigFile, genesisUpdates)
    } yield ()
  }

  private def writeToFile(configFile: File, oldConfigFile: File, genesisUpdates: GenesisUpdates) = {
    val writer          = new PrintWriter(configFile, "UTF-8")
    val oldConfigSource = Source.fromFile(oldConfigFile)

    import genesisUpdates._

    val pairsToUpdate = Map(
      "signature"                  -> UpdatesPairValue(signature),
      "genesis-public-key-base-58" -> UpdatesPairValue(signer),
      "block-timestamp"            -> UpdatesPairValue(blockTimestamp)
    )
    val lineFilter = produceFilteredLine(pairsToUpdate)(_)

    val writeResult = oldConfigSource
      .getLines()
      .foldLeft(Try(())) {
        case (prevResultMaybe, nextLine) => prevResultMaybe.flatMap(_ => Try(writer.println(lineFilter(nextLine))))
      }

    writeResult match {
      case Success(_) =>
        writer.flush()
        writer.close()
        oldConfigSource.close()
        Right {
          println(s"""Genesis signed successfully!
               |New configuration file: ${configFile.getAbsolutePath}

               |Old configuration saved to: ${oldConfigFile.getAbsolutePath}""".stripMargin)
        }
      case Failure(ex) =>
        writer.close()
        oldConfigSource
          .close()
        Left(ValidationError.GenericError(ex))
    }
  }

  private def produceFilteredLine(pairsToUpdate: Map[String, UpdatesPairValue[_]])(nextLine: String): String = {
    val delimiter = if (nextLine.contains(":")) ':' else '='
    val key       = nextLine.trim.takeWhile(_ != delimiter).trim
    (nextLine, pairsToUpdate.get(key)) match {
      case (line, Some(value)) =>
        val valueWithDelimiter = if (delimiter == ':') s": $value" else s" = $value"
        line.takeWhile(_ == ' ') + key + valueWithDelimiter
      case (line, _) => line
    }
  }

  private[generator] case class GenesisUpdates(signer: String, signature: String, blockTimestamp: Long)

  private case class UpdatesPairValue[T](value: T) {
    override def toString: String = value match {
      case v: String => v.mkString("\"", "", "\"")
      case z         => z.toString
    }
  }

  override def internalClose(): Unit = {
    time.close()
  }
}
