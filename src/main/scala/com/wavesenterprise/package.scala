package com

import com.typesafe.config.Config
import com.wavesenterprise.block.Block
import com.wavesenterprise.consensus.ConsensusPostAction
import com.wavesenterprise.settings._
import com.wavesenterprise.state.appender.BaseAppender.BlockType.Hard
import com.wavesenterprise.state.{ByteStr, NG}
import com.wavesenterprise.transaction.ValidationError.GenericError
import com.wavesenterprise.transaction.{BlockchainUpdater, ValidationError}
import com.wavesenterprise.utils.ScorexLogging
import pureconfig.ConfigSource

import java.io.File

package object wavesenterprise extends ScorexLogging {
  private def checkOrAppend(block: Block, blockchainUpdater: BlockchainUpdater with NG): Either[ValidationError, Unit] = {
    if (blockchainUpdater.isEmpty) {
      blockchainUpdater.processBlock(block, ConsensusPostAction.NoAction, Hard).right.map { _ =>
        log.info(s"Genesis block ${blockchainUpdater.blockHeaderAndSize(1).get._1} has been added to the state")
      }
    } else {
      val existingGenesisBlockId: Option[ByteStr] = blockchainUpdater.blockHeaderAndSize(1).map(_._1.signerData.signature)
      Either.cond(existingGenesisBlockId.fold(false)(_ == block.uniqueId),
                  (),
                  GenericError("Mismatched genesis blocks in configuration and blockchain"))
    }
  }

  def checkGenesis(settings: WESettings, blockchainUpdater: BlockchainUpdater with NG): Unit = {
    settings.blockchain.custom.genesis match {
      case plain: PlainGenesisSettings =>
        Block
          .genesis(plain, settings.blockchain.consensus.consensusType)
          .flatMap(b => checkOrAppend(b, blockchainUpdater))
          .left
          .foreach { e =>
            log.error("INCORRECT GENESIS BLOCK CONFIGURATION!!! NODE STOPPED BECAUSE OF THE FOLLOWING ERROR:")
            log.error(e.toString)
            com.wavesenterprise.utils.forceStopApplication()
          }
      case _: SnapshotBasedGenesisSettings =>
        if (blockchainUpdater.height < 1) {
          log.error("INCORRECT BLOCKCHAIN STATE!!! NODE STOPPED BECAUSE OF THE FOLLOWING ERROR:")
          log.error(s"No snapshot state found (configured genesis.type is '${GenesisType.SnapshotBased.entryName}')")
          com.wavesenterprise.utils.forceStopApplication()
        }
    }
  }

  /**
    * Try to parse node config file
    * Path to file must:
    *   - Be specified (None is not accepted anymore);
    *   - Exist;
    *   - Be a file, not a directory.
    *
    * Beware: in other cases we're faithlessly throwing an exception
    */
  def readConfigOrTerminate(maybeConfigPath: Option[String]): (Config, String) = {
    maybeConfigPath
      .map(configPathStr => new File(configPathStr) -> configPathStr) match {
      case Some((configFile, configPath)) if configFile.exists() && configFile.isFile =>
        val configSource = buildSourceBasedOnDefault(ConfigSource.file(configFile))
        val config = configSource.value() match {
          case Right(value) =>
            val config = value.toConfig
            if (config.hasPath("node")) {
              config
            } else {
              System.err.println("Malformed configuration file was provided! Aborting!")
              System.err.println("Please, read following article about configuration file format:")
              System.err.println("https://docs.wavesenterprise.com/how-to-setup/configuration-node.html")
              printAndThrowErr("Provided configuration file doesn't contain 'node' path")
            }
          case Left(failures) =>
            System.err.println("Failed to parse config:")
            printAndThrowErr(failures.prettyPrint())
        }

        config -> configPath

      case Some((configFile, configPath)) if configFile.isDirectory =>
        printAndThrowErr(s"Provided configuration file appeared to be a directory: $configPath")

      case Some((_, providedPath)) =>
        printAndThrowErr(s"Provided configuration file doesn't exist: $providedPath")

      case None =>
        printAndThrowErr("NO CONFIGURATION FILE WAS PROVIDED, ABORTING")
    }
  }

  private def printAndThrowErr(errorMessage: String): Nothing = {
    System.err.println(errorMessage)
    throw new IllegalArgumentException(errorMessage)
  }
}
