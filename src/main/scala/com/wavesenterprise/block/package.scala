package com.wavesenterprise

import com.google.common.io.{ByteArrayDataInput, ByteArrayDataOutput}
import com.google.common.primitives.Ints
import com.wavesenterprise.consensus._
import com.wavesenterprise.state.ByteStr
import com.wavesenterprise.transaction.{Transaction, TransactionParsers}
import com.wavesenterprise.utils.DatabaseUtils._

import java.nio.ByteBuffer

package object block {
  type DiscardedBlocks      = Seq[Block]
  type DiscardedMicroBlocks = Seq[MicroBlock]

  implicit class BlockByteArrayDataOutputExt(val output: ByteArrayDataOutput) {

    def writeHeader(blockHeader: BlockHeader): Unit = {
      writeVersion(blockHeader.version)
      writeTimestamp(blockHeader.timestamp)
      writeReference(blockHeader.reference)
      writeConsensus(blockHeader.consensusData)
      writeBlockTransactionCount(blockHeader.version, blockHeader.transactionCount)
      writeFeatures(blockHeader.version, blockHeader.featureVotes)
      writeGenesisData(blockHeader.version, blockHeader.genesisDataOpt)
      writeSignerData(blockHeader.signerData)
    }

    def writeBlock(block: Block, skipVotes: Boolean = false, skipSignature: Boolean = false): Unit = {
      writeVersion(block.version)
      writeTimestamp(block.timestamp)
      writeReference(block.reference)
      writeConsensus(block.consensusData, skipVotes)
      writeBlockTransactions(block.version, block.transactionData)
      writeFeatures(block.version, block.featureVotes)
      writeGenesisData(block.version, block.genesisDataOpt)
      writeSignerData(block.signerData, skipSignature)
    }

    def writeAsBlockBytes(blockHeader: BlockHeader, txsBytes: Array[Byte]): Unit = {
      writeVersion(blockHeader.version)
      writeTimestamp(blockHeader.timestamp)
      writeReference(blockHeader.reference)
      writeConsensus(blockHeader.consensusData)
      writeBlockTransactionsBytes(blockHeader.version, blockHeader.transactionCount, txsBytes)
      writeFeatures(blockHeader.version, blockHeader.featureVotes)
      writeGenesisData(blockHeader.version, blockHeader.genesisDataOpt)
      writeSignerData(blockHeader.signerData)
    }

    def writeVersion(version: Byte): Unit = output.writeByte(version)

    def writeTimestamp(timestamp: Long): Unit = output.writeLong(timestamp)

    def writeBlockTransactions(version: Byte, txs: Seq[Transaction]): Unit =
      writeTransactions(blockTxCountToBytes(version), txs)

    def writeMicroBlockTransactions(txs: Seq[Transaction]): Unit =
      writeTransactions(Ints.toByteArray, txs)

    private def writeTransactions(countWriter: Int => Array[Byte], txs: Seq[Transaction]): Unit = {
      val txCountBytes = countWriter(txs.size)
      val bytesLength  = txCountBytes.length + txs.foldLeft(0)((acc, tx) => acc + java.lang.Integer.BYTES + tx.bytes().length)
      output.writeInt(bytesLength)
      output.write(txCountBytes)
      txs.foreach { tx =>
        val txBytes = tx.bytes()
        output.writeInt(txBytes.length)
        output.write(txBytes)
      }
    }

    private def blockTxCountToBytes(version: Byte)(txCount: Int): Array[Byte] = {
      version match {
        case Block.LegacyGenesisBlockVersion | Block.PlainBlockVersion => Array(txCount.toByte)
        case Block.NgBlockVersion | Block.ModernGenesisBlockVersion    => Ints.toByteArray(txCount)
      }
    }

    def writeVotes(votes: Seq[Vote]): Unit = {
      output.writeShort(votes.length)
      votes.foreach(vote => output.write(vote.bytes()))
    }

    private def writeReference(reference: ByteStr): Unit = output.writeByteStr(reference)

    private def writeConsensus(consensusBlockData: ConsensusBlockData, skipVotes: Boolean = false): Unit = {
      consensusBlockData match {
        case posData: PoSLikeConsensusBlockData =>
          val bytesLength = 1 + Block.BaseTargetLength + Block.GeneratorSignatureLength
          output.writeInt(bytesLength)
          output.writeByte(PoSLikeConsensusBlockData.typeByte)
          output.writeLong(posData.baseTarget)
          output.write(posData.generationSignature.arr)
        case poaData: PoALikeConsensusBlockData =>
          val bytesLength = 1 + Block.OverallSkippedRoundsLength
          output.writeInt(bytesLength)
          output.writeByte(PoALikeConsensusBlockData.typeByte)
          output.writeLong(poaData.overallSkippedRounds)
        case cftData: CftLikeConsensusBlockData =>
          val resultVotes = if (skipVotes) Seq.empty else cftData.votes
          val bytesLength = 1 + Block.VotesArraySizeLength + Block.VoteLength * resultVotes.length + Block.OverallSkippedRoundsLength
          output.writeInt(bytesLength)
          output.writeByte(CftLikeConsensusBlockData.typeByte)
          writeVotes(resultVotes)
          output.writeLong(cftData.overallSkippedRounds)
      }
    }

    private def writeFeatures(version: Byte, features: Set[Short]): Unit = {
      version match {
        case v if v > 2 =>
          output.writeInt(features.size)
          features.foreach(feature => output.writeShort(feature))
        case _ =>
      }
    }

    private def writeSignerData(signerData: SignerData, skipSignature: Boolean = false): Unit = {
      output.writePublicKey(signerData.generator)
      if (!skipSignature) output.writeByteStr(signerData.signature)
    }

    private def writeGenesisData(version: Byte, maybeGenesisData: Option[GenesisData]): Unit = {
      version match {
        case Block.ModernGenesisBlockVersion =>
          maybeGenesisData.fold(output.writeByte(0)) { data =>
            output.writeByte(1)
            output.writeBoolean(data.senderRoleEnabled)
          }
        case _ => ()
      }
    }

    private def writeBlockTransactionCount(version: Byte, count: Int): Unit = output.write(blockTxCountToBytes(version)(count))

    private def writeBlockTransactionsBytes(version: Byte, txCount: Int, txsBytes: Array[Byte]): Unit = {
      val txCountBytes = blockTxCountToBytes(version)(txCount)
      val bytesLength  = txCountBytes.length + txsBytes.length
      output.writeInt(bytesLength)
      output.write(txCountBytes)
      output.write(txsBytes)
    }
  }

  implicit class BlockByteArrayDataInputExt(val input: ByteArrayDataInput) extends AnyVal {

    def readVersion: Byte = input.readByte()

    def readTimestamp: Long = input.readLong()

    def readSignature: ByteStr = input.readByteStr(crypto.SignatureLength)

    def readHash: ByteStr = input.readByteStr(crypto.DigestSize)

    def readConsensusBlockData: ConsensusBlockData = {
      input.readInt() // Consensus block length not used
      input.readByte() match {
        case PoSLikeConsensusBlockData.typeByte =>
          val baseTarget          = input.readLong
          val generationSignature = ByteStr(input.readBytes(Block.GeneratorSignatureLength))
          PoSLikeConsensusBlockData(baseTarget, generationSignature)
        case PoALikeConsensusBlockData.typeByte =>
          val overallSkippedRoundsLength = input.readLong()
          PoALikeConsensusBlockData(overallSkippedRoundsLength)
        case CftLikeConsensusBlockData.typeByte =>
          val votes                      = readVotes()
          val overallSkippedRoundsLength = input.readLong()
          CftLikeConsensusBlockData(votes, overallSkippedRoundsLength)
      }
    }

    def readVotes(): Vector[Vote] = {
      val votesCount = input.readShort()
      (1 to votesCount).view.map { _ =>
        Vote.parseBytes(input.readBytes(Block.VoteLength)).get
      }.toVector
    }

    def readFeatures(version: Int): Set[Short] = {
      if (version > 2) {
        val featureVotesCount = input.readInt()
        List.fill(featureVotesCount)(input.readShort()).toSet
      } else
        Set.empty[Short]
    }

    def readGenesisData(version: Int): Option[GenesisData] = {
      version match {
        case Block.ModernGenesisBlockVersion =>
          if (input.readByte() == 0) {
            None
          } else {
            Some(GenesisData(input.readBoolean()))
          }
        case _ => None
      }
    }

    def readBlockTransactionCount(version: Byte): Int = {
      version match {
        case Block.PlainBlockVersion | Block.LegacyGenesisBlockVersion => input.readByte()
        case Block.NgBlockVersion | Block.ModernGenesisBlockVersion    => input.readInt()
      }
    }

    def readBlockTransactionBytes(version: Byte): (Int, Array[Byte]) = {
      val txBytesLength = input.readInt()
      val txBytes       = input.readBytes(txBytesLength)
      val txCount = version match {
        case Block.PlainBlockVersion | Block.LegacyGenesisBlockVersion => txBytes.head
        case Block.NgBlockVersion | Block.ModernGenesisBlockVersion    => ByteBuffer.wrap(txBytes, 0, 4).getInt()
      }
      (txCount, txBytes)
    }

    def readMicroBlockTransactions: Seq[Transaction] = {
      input.readInt() // bytes length not used
      val txCount = input.readInt()
      (1 to txCount).map { _ =>
        val transactionLength = input.readInt()
        TransactionParsers.parseBytes(input.readBytes(transactionLength)).get
      }
    }

    def readHeaderAndTx: (BlockHeader, Array[Byte]) = {
      readBlockHeader(withTx = true)
    }

    def readHeader: BlockHeader = {
      readBlockHeader(withTx = false)._1
    }

    private def readBlockHeader(withTx: Boolean): (BlockHeader, Array[Byte]) = {
      val version       = input.readVersion
      val timestamp     = input.readTimestamp
      val reference     = input.readSignature
      val consensusData = input.readConsensusBlockData
      val (txCount, txBytes) = {
        if (withTx)
          input.readBlockTransactionBytes(version)
        else
          (input.readBlockTransactionCount(version), Array.empty[Byte])
      }
      val supportedFeaturesIds = input.readFeatures(version)
      val maybeGenesisData     = input.readGenesisData(version)
      val generator            = input.readPublicKey
      val signature            = input.readSignature
      val blockHeader = new BlockHeader(timestamp,
                                        version,
                                        reference,
                                        SignerData(generator, signature),
                                        consensusData,
                                        supportedFeaturesIds,
                                        maybeGenesisData,
                                        txCount)
      (blockHeader, txBytes)
    }
  }
}
