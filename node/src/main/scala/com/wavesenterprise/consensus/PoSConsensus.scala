package com.wavesenterprise.consensus

import cats.implicits._
import com.wavesenterprise.block.Block
import com.wavesenterprise.crypto
import com.wavesenterprise.features.BlockchainFeature
import com.wavesenterprise.features.FeatureProvider.FeatureProviderExt
import com.wavesenterprise.settings.BlockchainSettings
import com.wavesenterprise.state.{Blockchain, ByteStr, _}
import com.wavesenterprise.transaction.ValidationError
import com.wavesenterprise.transaction.ValidationError.{BlockFromFuture, GenericError}
import com.wavesenterprise.utils.Base58

class PoSConsensus(blockchain: Blockchain, settings: BlockchainSettings) extends Consensus {

  import PoSConsensus._

  private val genesisSettings = settings.custom.genesis.toPlainSettingsUnsafe

  def blockConsensusValidation(currentTimestamp: => Long, block: Block): Either[ValidationError, ConsensusPostAction] = {

    val genBalance = { height: Int =>
      val balance = GeneratingBalanceProvider.balance(blockchain, settings.custom.functionality, height, block.sender.toAddress)
      Either.cond(
        balance >= GeneratingBalanceProvider.MinimalEffectiveBalance,
        balance,
        s"generator's effective balance $balance is less that required for generation"
      )
    }

    val blockTime = block.timestamp

    for {
      height <- blockchain.heightOf(block.reference).toRight(GenericError(s"height: history does not contain parent ${block.reference}"))
      parent <- blockchain.parent(block).toRight(GenericError(s"parent: history does not contain parent ${block.reference}"))
      grandParent = blockchain.parent(parent, 2)
      effectiveBalance <- genBalance(height).left.map(GenericError(_))
      _                <- validateBlockVersion(height, block, settings.custom.functionality)
      currentTs = currentTimestamp
      _ <- Either.cond(blockTime - currentTs < MaxTimeDrift, (), BlockFromFuture(blockTime, currentTs))
      _ <- validateBaseTarget(height, block, parent, grandParent)
      _ <- validateGeneratorSignature(height, block)
      _ <- validateBlockDelay(height, block, parent, effectiveBalance)
    } yield ConsensusPostAction.NoAction
  }.left.map {
    case GenericError(x) => GenericError(s"Block $block is invalid: $x")
    case x               => x
  }

  def calculatePostAction(block: Block): Either[ValidationError, ConsensusPostAction] =
    Right(ConsensusPostAction.NoAction)

  def consensusData(accountPublicKey: Array[Byte],
                    height: Int,
                    refBlockBT: Long,
                    refBlockTS: Long,
                    greatGrandParentTS: Option[Long],
                    currentTime: Long): Either[ValidationError, PoSLikeConsensusBlockData] = {
    val isPosFixActivated = blockchain.isFeatureActivated(BlockchainFeature.ConsensusFix, height)
    val bt = PoSConsensus.calculateBaseTarget(
      height,
      refBlockBT,
      refBlockTS,
      greatGrandParentTS,
      genesisSettings.averageBlockDelay.toSeconds,
      currentTime,
      isPosFixActivated
    )
    blockchain.lastBlock
      .toRight(GenericError("No blocks in blockchain"))
      .flatMap(lastBlock => lastBlock.consensusData.asPoSMaybe())
      .map(consensusData => PoSLikeConsensusBlockData(bt, ByteStr(generatorSignature(consensusData.generationSignature.arr, accountPublicKey))))
  }

  private val NormCoefficient: Double = 0.2 * 1e15 / genesisSettings.initialBalance.units

  def getValidBlockDelay(height: Int, accountPublicKey: Array[Byte], refBlockBT: Long, balance: Long): Either[ValidationError, Long] = {
    getHit(height, accountPublicKey)
      .map(PoSConsensus.calculateDelay(_, refBlockBT, balance, NormCoefficient))
      .toRight(GenericError("No blocks in blockchain"))
  }

  def validateBlockDelay(height: Int, block: Block, parent: Block, effectiveBalance: Long): Either[ValidationError, Unit] = {
    parent.consensusData
      .asPoSMaybe()
      .flatMap(posData => getValidBlockDelay(height, block.signerData.generator.publicKey.getEncoded, posData.baseTarget, effectiveBalance))
      .map(_ + parent.timestamp)
      .ensureOr(mvt => GenericError(s"Block timestamp ${block.timestamp} less than min valid timestamp $mvt"))(ts => ts <= block.timestamp)
      .map(_ => ())
  }

  def validateGeneratorSignature(height: Int, block: Block): Either[ValidationError, Unit] =
    for {
      blockGS     <- block.consensusData.asPoSMaybe().map(_.generationSignature.arr)
      lastBlock   <- blockchain.lastBlock.toRight(GenericError("No blocks in blockchain"))
      lastBlockGS <- lastBlock.consensusData.asPoSMaybe().map(_.generationSignature.arr)
      genSignature = generatorSignature(lastBlockGS, block.signerData.generator.publicKey.getEncoded)
      _ <- Either.cond(
        genSignature sameElements blockGS,
        (),
        GenericError(s"Generation signatures does not match: Expected = ${Base58.encode(genSignature)}; Found = ${Base58.encode(blockGS)}")
      )
    } yield ()

  def validateBaseTarget(height: Int, block: Block, parent: Block, grandParent: Option[Block]): Either[ValidationError, Unit] =
    for {
      blockBT <- block.consensusData.asPoSMaybe().map(_.baseTarget)
      blockTS = block.timestamp

      parentBT <- parent.consensusData.asPoSMaybe().map(_.baseTarget)
      isPosFixActivated = blockchain.isFeatureActivated(BlockchainFeature.ConsensusFix, height)

      expectedBT = PoSConsensus.calculateBaseTarget(
        height,
        parentBT,
        parent.timestamp,
        grandParent.map(_.timestamp),
        genesisSettings.averageBlockDelay.toSeconds,
        blockTS,
        isPosFixActivated
      )

      _ <- Either.cond(expectedBT == blockBT, (), GenericError(s"declared baseTarget $blockBT does not match calculated baseTarget $expectedBT"))
    } yield ()

  private def getHit(height: Int, accountPublicKey: Array[Byte]): Option[BigInt] = {
    val blockForHit =
      if (height > 100) blockchain.blockAt(height - 100)
      else blockchain.lastBlock

    blockForHit
      .flatMap(_.consensusData.asPoSMaybe().toOption)
      .map { consensusData =>
        val genSig = consensusData.generationSignature.arr
        hit(generatorSignature(genSig, accountPublicKey))
      }
  }
}

object PoSConsensus {

  private[consensus] val HitSize: Int            = 8
  private[consensus] val MinBaseTarget: Long     = 9
  private[consensus] val ScoreAdjustmentConstant = BigInt("18446744073709551616")

  private[consensus] def generatorSignature(signature: Array[Byte], publicKey: Array[Byte]): Array[Byte] = {
    val s = new Array[Byte](crypto.DigestSize * 2)
    System.arraycopy(signature, 0, s, 0, crypto.DigestSize)
    System.arraycopy(publicKey, 0, s, crypto.DigestSize, crypto.DigestSize)
    crypto.fastHash(s)
  }

  private[consensus] def hit(generatorSignature: Array[Byte]): BigInt = BigInt(1, generatorSignature.take(HitSize).reverse)

  /**
    * WARNING: Legacy normalize method, used by Waves in PoS
    * Contains hardcode, that doesn't work for settings of averageBlockDelay other than '60 seconds'
    */
  private[consensus] def legacyNormalize(value: Long, targetBlockDelaySeconds: Long): Double =
    value * targetBlockDelaySeconds / (60: Double)

  private[consensus] def normalizeBaseTarget(baseTarget: Long, targetBlockDelaySeconds: Long): Long = {
    baseTarget
      .max(MinBaseTarget)
      .min(Long.MaxValue / targetBlockDelaySeconds)
  }

  private val MaxSignature: Array[Byte] = Array.fill[Byte](HitSize)(-1)
  private val MaxHit: BigDecimal        = BigDecimal(BigInt(1, MaxSignature))
  private val C1                        = 70000
  private val C2                        = 5e17
  private val TMin                      = 5000

  def calculateDelay(hit: BigInt, bt: Long, balance: Long, normCoefficient: Double): Long = {
    val h = (BigDecimal(hit) / MaxHit).toDouble
    val a = TMin + C1 * math.log(1 - C2 * math.log(h) / bt / (balance * normCoefficient))
    a.toLong
  }

  def calculateBaseTarget(prevHeight: Int,
                          prevBaseTarget: Long,
                          parentTimestamp: Long,
                          maybeGreatGrandParentTimestamp: Option[Long],
                          averageBlockDelaySeconds: Long,
                          timestamp: Long,
                          isConsensusFixActivated: Boolean = false): Long = {
    val bounds =
      if (isConsensusFixActivated) {
        DelayBounds(
          minDelaySeconds = 0.5 * averageBlockDelaySeconds.toDouble,
          maxDelaySeconds = 1.5 * averageBlockDelaySeconds.toDouble
        )
      } else {
        // Waves-like delay calculation
        DelayBounds(
          minDelaySeconds = legacyNormalize(30, averageBlockDelaySeconds),
          maxDelaySeconds = legacyNormalize(90, averageBlockDelaySeconds)
        )
      }

    maybeGreatGrandParentTimestamp match {
      case None =>
        prevBaseTarget
      case Some(ts) =>
        val avg = (timestamp - ts) / 3 / 1000
        if (avg > bounds.maxDelaySeconds) {
          prevBaseTarget + math.max(1, prevBaseTarget / 100)
        } else if (avg < bounds.minDelaySeconds) {
          prevBaseTarget - math.max(1, prevBaseTarget / 100)
        } else {
          prevBaseTarget
        }
    }
  }

  def calculateBlockScore(baseTarget: Long): BigInt = {
    (ScoreAdjustmentConstant / baseTarget).ensuring(_ > 0)
  }

  private case class DelayBounds(minDelaySeconds: Double, maxDelaySeconds: Double)
}
