package com.wavesenterprise

import com.typesafe.config.{Config, ConfigFactory}
import com.wavesenterprise.account.{Address, PrivateKeyAccount}
import com.wavesenterprise.acl.Role
import com.wavesenterprise.block.{Block, TxMicroBlock}
import com.wavesenterprise.consensus.PoSLikeConsensusBlockData
import com.wavesenterprise.features.BlockchainFeature
import com.wavesenterprise.lagonaki.mocks.TestBlock
import com.wavesenterprise.settings.{BlockchainSettings, ConsensusSettings, Custom, FeeSettings, TestFunctionalitySettings, WESettings}
import com.wavesenterprise.state._
import com.wavesenterprise.utils.EitherUtils.EitherExt
import com.wavesenterprise.transaction.{GenesisPermitTransaction, Transaction}
import pureconfig.ConfigSource

import scala.util.Try

package object history {
  val MaxTransactionsPerBlockDiff = 10
  val MaxBlocksInMemory           = 5
  val DefaultBaseTarget           = 1000L

  val config: Config = ConfigFactory.parseString(s"""node {
       |  owner-address = ${crypto.generatePublicKey.toAddress}
       |}
     """.stripMargin).withFallback(ConfigFactory.load())

  val settings: WESettings = ConfigSource
    .fromConfig(config)
    .at(WESettings.configPath)
    .loadOrThrow[WESettings]

  val DefaultBlockchainSettings = BlockchainSettings(
    Custom(TestFunctionalitySettings.Enabled, settings.blockchain.custom.genesis, settings.blockchain.custom.addressSchemeCharacter),
    ConfigSource.fromConfig(config.getConfig("node.blockchain.fees")).loadOrThrow[FeeSettings],
    ConsensusSettings.PoSSettings
  )

  val MicroblocksActivatedAt0BlockchainSettings: BlockchainSettings =
    DefaultBlockchainSettings.copy(
      custom = DefaultBlockchainSettings.custom.copy(
        functionality = DefaultBlockchainSettings.custom.functionality
          .copy(preActivatedFeatures = Map(BlockchainFeature.NG.id -> 0, BlockchainFeature.SmartAccounts.id -> 0))
      )
    )

  val MicroblocksActivatedAt0WESettings: WESettings = settings.copy(blockchain = MicroblocksActivatedAt0BlockchainSettings)

  val DefaultWESettings: WESettings =
    settings.copy(blockchain = DefaultBlockchainSettings, features = settings.features.copy(autoShutdownOnUnsupportedFeature = false))

  val keyPair             = crypto.generateKeyPair()
  val defaultSigner       = PrivateKeyAccount(keyPair)
  val generationSignature = ByteStr(Array.fill(Block.GeneratorSignatureLength)(0: Byte))

  def buildGenesisPermitTxs(startTimestamp: Long, addressToRole: Map[Address, Role]): Seq[GenesisPermitTransaction] = {
    addressToRole.zipWithIndex.map {
      case ((address, role), timestampShift) =>
        GenesisPermitTransaction.create(address, role, startTimestamp + timestampShift).explicitGet()
    }.toSeq
  }

  def buildBlockOfTxs(refTo: ByteStr, txs: Seq[Transaction]): Block = {
    val txCount        = txs.length
    val avgTxTimestamp = txs.map(_.timestamp / txCount.toLong).sum
    customBuildBlockOfTxs(refTo, txs, defaultSigner, 1, avgTxTimestamp)
  }

  def buildBlockOfTxs(refTo: ByteStr, txs: Seq[Transaction], timestamp: Long): Block = customBuildBlockOfTxs(refTo, txs, defaultSigner, 1, timestamp)

  def customBuildBlockOfTxs(refTo: ByteStr,
                            txs: Seq[Transaction],
                            signer: PrivateKeyAccount,
                            version: Byte,
                            timestamp: Long,
                            bTarget: Long = DefaultBaseTarget): Block =
    Block
      .buildAndSign(
        version = version,
        timestamp = timestamp,
        reference = refTo,
        consensusData = PoSLikeConsensusBlockData(baseTarget = bTarget, generationSignature = generationSignature),
        transactionData = txs,
        signer = signer,
        Set.empty
      )
      .explicitGet()

  def customBuildMicroBlockOfTxs(totalRefTo: ByteStr,
                                 prevTotal: Block,
                                 txs: Seq[Transaction],
                                 signer: PrivateKeyAccount,
                                 version: Byte,
                                 ts: Long): (Block, TxMicroBlock) = {
    val newTotalBlock = customBuildBlockOfTxs(totalRefTo, prevTotal.transactionData ++ txs, signer, version, ts)
    val nonSigned = TxMicroBlock
      .buildAndSign(
        generator = signer,
        timestamp = ts,
        transactionData = txs,
        prevResBlockSig = prevTotal.uniqueId,
        totalResBlockSig = newTotalBlock.uniqueId
      )
      .explicitGet()
    (newTotalBlock, nonSigned)
  }

  def buildMicroBlockOfTxs(totalRefTo: ByteStr, prevTotal: Block, txs: Seq[Transaction], signer: PrivateKeyAccount): (Block, TxMicroBlock) = {
    val newTotalBlock = buildBlockOfTxs(totalRefTo, prevTotal.transactionData ++ txs)
    val nonSigned = TxMicroBlock
      .buildAndSign(
        generator = signer,
        timestamp = Try(txs.map(_.timestamp).max).getOrElse(System.currentTimeMillis()),
        transactionData = txs,
        prevResBlockSig = prevTotal.uniqueId,
        totalResBlockSig = newTotalBlock.uniqueId
      )
      .explicitGet()
    (newTotalBlock, nonSigned)
  }

  def randomSig: ByteStr = TestBlock.randomOfLength(Block.BlockIdLength)

  def chainBlocks(txs: Seq[Seq[Transaction]]): Seq[Block] = {
    def chainBlocksR(refTo: ByteStr, txs: Seq[Seq[Transaction]]): Seq[Block] = txs match {
      case (x :: xs) =>
        val txCount        = x.length
        val avgTxTimestamp = x.map(_.timestamp / txCount.toLong).sum
        val block          = buildBlockOfTxs(refTo, x, avgTxTimestamp)
        block +: chainBlocksR(block.uniqueId, xs)
      case _ => Seq.empty
    }

    chainBlocksR(randomSig, txs)
  }

  def chainBaseAndMicro(totalRefTo: ByteStr,
                        baseTxs: Seq[Transaction],
                        micros: Seq[Seq[Transaction]],
                        signer: PrivateKeyAccount = defaultSigner): (Block, Seq[TxMicroBlock]) = {
    val baseTimestamp = baseTxs.map(_.timestamp).max + 1
    chainBaseAndMicro(totalRefTo, baseTxs, micros.map(txs => MicroInfoForChain(txs, baseTimestamp)), baseTimestamp, signer)
  }

  def chainBaseAndMicro(totalRefTo: ByteStr,
                        base: Seq[Transaction],
                        microInfos: Seq[MicroInfoForChain],
                        baseTimestamp: Long,
                        signer: PrivateKeyAccount): (Block, Seq[TxMicroBlock]) = {
    val block = customBuildBlockOfTxs(totalRefTo, base, signer, Block.NgBlockVersion, baseTimestamp)
    val microBlocks = microInfos
      .foldLeft((block, Seq.empty[TxMicroBlock])) {
        case ((lastTotal, allMicros), MicroInfoForChain(txs, microTimestamp)) =>
          val (newTotal, micro) = customBuildMicroBlockOfTxs(totalRefTo, lastTotal, txs, signer, Block.NgBlockVersion, microTimestamp)
          (newTotal, allMicros :+ micro)
      }
      ._2
    (block, microBlocks)
  }

  def spoilSignature(b: Block): Block = {
    val newBlockHeader = b.blockHeader.copy(signerData = b.blockHeader.signerData.copy(signature = TestBlock.randomSignature()))
    b.copy(blockHeader = newBlockHeader)
  }
}
