package com.wavesenterprise

import java.io.IOException
import java.nio.file.attribute.BasicFileAttributes
import java.nio.file.{FileVisitResult, Files, Path, SimpleFileVisitor}

import com.wavesenterprise.account.{PrivateKeyAccount, PublicKeyAccount}
import com.wavesenterprise.acl.Role
import com.wavesenterprise.block.Block
import com.wavesenterprise.consensus.PoSLikeConsensusBlockData
import com.wavesenterprise.crypto.SignatureLength
import com.wavesenterprise.settings.{PlainGenesisSettings, GenesisTransactionSettings, NetworkParticipantDescription, WestAmount}
import com.wavesenterprise.state.ByteStr
import com.wavesenterprise.utils.Base58

import scala.concurrent.duration._

object TestHelpers {
  def buildGenesis(signer: PrivateKeyAccount,
                   balances: Map[PublicKeyAccount, Long],
                   blockTimestamp: Long = System.currentTimeMillis()): PlainGenesisSettings = {

    val totalAmount = balances.values.sum

    val genesisTransferDescriptions = balances.toSeq.map {
      case (account, amount) =>
        GenesisTransactionSettings(account.toAddress, WestAmount(amount))
    }

    val participantTxDescriptions = {
      NetworkParticipantDescription(signer.publicKeyBase58, Seq(Role.Permissioner.prefixS)) +:
        balances.toSeq.map {
          case (key, _) => NetworkParticipantDescription(key.publicKeyBase58, Seq.empty)
        }
    }

    val genesisTxs = Block.genesisTransactions(genesisTransferDescriptions, participantTxDescriptions, blockTimestamp)

    val genesisBlock = Block
      .buildAndSign(
        version = 1,
        timestamp = blockTimestamp,
        reference = ByteStr(Array.fill(SignatureLength)(-1: Byte)),
        consensusData = PoSLikeConsensusBlockData(1000, ByteStr(Array.fill(crypto.DigestSize)(0: Byte))),
        transactionData = genesisTxs,
        signer = signer,
        featureVotes = Set.empty
      )
      .right
      .get

    PlainGenesisSettings(
      blockTimestamp,
      WestAmount(totalAmount),
      Base58.encode(signer.publicKey.getEncoded),
      Some(genesisBlock.signerData.signature),
      genesisTransferDescriptions,
      participantTxDescriptions,
      1000,
      60.seconds
    )
  }

  def deleteRecursively(path: Path): Unit = Files.walkFileTree(
    path,
    new SimpleFileVisitor[Path] {
      override def postVisitDirectory(dir: Path, exc: IOException): FileVisitResult = {
        Option(exc).fold {
          Files.delete(dir)
          FileVisitResult.CONTINUE
        }(throw _)
      }

      override def visitFile(file: Path, attrs: BasicFileAttributes): FileVisitResult = {
        Files.delete(file)
        FileVisitResult.CONTINUE
      }
    }
  )
}
