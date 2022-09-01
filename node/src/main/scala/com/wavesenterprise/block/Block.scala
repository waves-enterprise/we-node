package com.wavesenterprise.block

import cats.Monoid
import cats.implicits.{catsKernelStdMonoidForMap, catsSyntaxOption}
import cats.syntax.either._
import com.google.common.io.ByteStreams.{newDataInput, newDataOutput}
import com.google.common.primitives.Ints
import com.wavesenterprise.account.{Address, PrivateKeyAccount, PublicKeyAccount}
import com.wavesenterprise.acl.Role
import com.wavesenterprise.consensus._
import com.wavesenterprise.crypto
import com.wavesenterprise.crypto.{DigestSize, KeyLength, SignatureLength}
import com.wavesenterprise.network.BlockWrapper
import com.wavesenterprise.settings.{
  ConsensusType,
  GenesisSettings,
  GenesisSettingsVersion,
  GenesisTransactionSettings,
  NetworkParticipantDescription,
  PlainGenesisSettings
}
import com.wavesenterprise.state.Portfolio.Fraction
import com.wavesenterprise.state._
import com.wavesenterprise.transaction.ValidationError.GenericError
import com.wavesenterprise.transaction.docker.{ExecutedContractTransaction, ExecutedContractTransactionV2}
import com.wavesenterprise.transaction.{GenesisPermitTransaction, _}
import com.wavesenterprise.utils.EitherUtils.EitherExt
import com.wavesenterprise.utils.{Base58, ScorexLogging}
import monix.eval.Coeval
import play.api.libs.json.{JsArray, JsObject, Json, OWrites}

import java.nio.ByteBuffer
import scala.util.control.NonFatal
import scala.util.{Failure, Try}

case class GenesisData(senderRoleEnabled: Boolean)

object GenesisData {
  implicit val writes: OWrites[GenesisData] = Json.writes[GenesisData]
}

case class BlockHeader(override val timestamp: Long,
                       override val version: Byte,
                       override val reference: ByteStr,
                       override val signerData: SignerData,
                       override val consensusData: ConsensusBlockData,
                       override val featureVotes: Set[Short],
                       override val genesisDataOpt: Option[GenesisData],
                       transactionCount: Int)
    extends BlockFields(timestamp, version, reference, signerData, consensusData, featureVotes, genesisDataOpt) {

  val json: Coeval[JsObject] = Coeval.evalOnce(BlockHeaderJsonify.json(this))

  val bytes: Coeval[Array[Byte]] = Coeval.evalOnce {
    val ndo = newDataOutput()
    ndo.writeHeader(this)
    ndo.toByteArray
  }
}

object BlockHeader extends ScorexLogging {

  def parse(bytes: Array[Byte]): BlockHeader = {
    val input = newDataInput(bytes)
    input.readHeader
  }

  def json(bh: BlockHeader, blockSize: Int): JsObject =
    bh.json() ++ Json.obj(
      "blocksize"        -> blockSize,
      "transactionCount" -> bh.transactionCount
    )
}

case class Block private (blockHeader: BlockHeader, transactionData: Seq[Transaction])
    extends BlockFields(
      blockHeader.timestamp,
      blockHeader.version,
      blockHeader.reference,
      blockHeader.signerData,
      blockHeader.consensusData,
      blockHeader.featureVotes,
      blockHeader.genesisDataOpt
    )
    with Signed
    with BlockWrapper {

  override val block: Block = this

  val bytes: Coeval[Array[Byte]] = Coeval.evalOnce {
    val ndo = newDataOutput()
    ndo.writeBlock(this)
    ndo.toByteArray
  }

  val feesPortfolio: Coeval[Map[Address, Portfolio]] = Coeval.evalOnce(BlockFeeCalculator.feesPortfolio(this))

  val blockFee: Coeval[Long] = Coeval.evalOnce(Monoid[Portfolio].combineAll(feesPortfolio().values).balance)

  val prevBlockFeePart: Coeval[Portfolio] = Coeval.evalOnce(BlockFeeCalculator.prevBlockFeePart(this))

  val json: Coeval[JsObject] = Coeval.evalOnce {
    BlockHeader.json(blockHeader, bytes().length) ++
      Json.obj("fee" -> blockFee(), "transactions" -> JsArray(transactionData.map(_.json())))
  }

  def bytesWithoutSignatureAndVotes(): Array[Byte] = {
    val ndo = newDataOutput()
    ndo.writeBlock(this, skipVotes = true, skipSignature = true)
    ndo.toByteArray
  }

  val votingHash: Coeval[ByteStr] = Coeval.evalOnce {
    ByteStr(crypto.fastHash(bytesWithoutSignatureAndVotes()))
  }

  def bytesWithoutSignature(): Array[Byte] = bytes().dropRight(SignatureLength)

  val blockScore: Coeval[BigInt] = Coeval.evalOnce {
    consensusData match {
      case PoSLikeConsensusBlockData(baseTarget, _)    => PoSConsensus.calculateBlockScore(baseTarget)
      case PoALikeConsensusBlockData(skippedRounds)    => PoALikeConsensus.calculateBlockScore(skippedRounds, timestamp)
      case CftLikeConsensusBlockData(_, skippedRounds) => PoALikeConsensus.calculateBlockScore(skippedRounds, timestamp)
    }
  }

  override val signatureValid: Coeval[Boolean] = Coeval.evalOnce {
    crypto.verify(signerData.signature.arr, bytesWithoutSignature(), signerData.generator.publicKey)
  }

  override def toString: String =
    s"Block(${signerData.signature} -> $reference, version=$version, signer=${signerData.generator}, txs=${transactionData.size}, features=$featureVotes, ts=$timestamp)"
}

object Block extends ScorexLogging {

  type BlockIds = Seq[ByteStr]
  type BlockId  = ByteStr

  val LegacyGenesisBlockVersion: Byte = 1
  val PlainBlockVersion: Byte         = 2
  val NgBlockVersion: Byte            = 3
  val ModernGenesisBlockVersion: Byte = 4
  val GenesisBlockVersions: Set[Byte] = Set(LegacyGenesisBlockVersion, ModernGenesisBlockVersion)

  val AllBlockVersions: Set[Byte] = Set(LegacyGenesisBlockVersion, PlainBlockVersion, NgBlockVersion, ModernGenesisBlockVersion)

  val MaxTransactionsPerBlockVer1Ver2: Int = Byte.MaxValue
  val MaxTransactionsPerBlockVer3: Int     = 6000
  val MaxFeaturesInBlock: Int              = 64
  val BaseTargetLength: Int                = 8
  val OverallSkippedRoundsLength: Int      = 8
  val GeneratorSignatureLength: Int        = 32
  val VoteLength: Int                      = KeyLength + DigestSize + SignatureLength
  val VotesArraySizeLength: Int            = 2

  val BlockIdLength: Int = SignatureLength

  val TransactionSizeLength = 4

  def parseBlockTxBytes(version: Int, bytes: Array[Byte]): Try[Seq[Transaction]] = Try {
    if (bytes.isEmpty) {
      Seq.empty
    } else {
      val (txsBytes: Array[Byte], count: Int) = version match {
        case LegacyGenesisBlockVersion | PlainBlockVersion => (bytes.tail, bytes.head.toInt) //  127 max, won't work properly if greater
        case NgBlockVersion | ModernGenesisBlockVersion =>
          val size = ByteBuffer.wrap(bytes, 0, 4).getInt()
          (bytes.drop(4), size)
        case _ => throw new NotImplementedError("Unsupported block version")
      }

      val txs = Seq.newBuilder[Transaction]
      (1 to count).foldLeft(0) {
        case (pos, _) =>
          val transactionLengthBytes = txsBytes.slice(pos, pos + TransactionSizeLength)
          val transactionLength      = Ints.fromByteArray(transactionLengthBytes)
          val transactionBytes       = txsBytes.slice(pos + TransactionSizeLength, pos + TransactionSizeLength + transactionLength)
          txs += TransactionParsers.parseBytes(transactionBytes).get
          pos + TransactionSizeLength + transactionLength
      }

      txs.result()
    }
  }

  private def parseHeaderAndTx(bytes: Array[Byte]): Try[(BlockHeader, Array[Byte])] =
    Try {
      val input = newDataInput(bytes)
      input.readHeaderAndTx
    }.recoverWith {
      case NonFatal(t) =>
        log.error("Error when parsing block", t)
        Failure(t)
    }

  def parseBytes(bytes: Array[Byte]): Try[Block] =
    for {
      (blockHeader, transactionBytes) <- Block.parseHeaderAndTx(bytes)
      transactionsData                <- parseBlockTxBytes(blockHeader.version, transactionBytes)
      block <- build(
        blockHeader.version,
        blockHeader.timestamp,
        blockHeader.reference,
        blockHeader.consensusData,
        transactionsData,
        blockHeader.signerData,
        blockHeader.featureVotes
      ).left.map(ve => new IllegalArgumentException(ve.toString)).toTry
    } yield block

  def build(h: BlockHeader, txs: Seq[Transaction]): Either[GenericError, Block] = {
    (for {
      _ <- Either.cond(AllBlockVersions.contains(h.version), (), s"Block version is incorrect: found '${h.version}', available '$AllBlockVersions'")
      _ <- Either.cond(h.reference.arr.length == SignatureLength, (), "Incorrect reference")
      _ <- h.consensusData match {
        case PoSLikeConsensusBlockData(_, generationSignature) =>
          Either.cond(generationSignature.arr.length == GeneratorSignatureLength, (), "Incorrect consensusData.generationSignature")
        case _: PoALikeConsensusBlockData =>
          Right(())
        case CftLikeConsensusBlockData(_, _) =>
          Right(())
      }
      _ <- Either.cond(h.signerData.generator.publicKey.getEncoded.length == KeyLength, (), "Incorrect signer.publicKey")
      _ <- Either.cond(h.version > PlainBlockVersion || h.featureVotes.isEmpty, (), s"Block version '${h.version}' could not contain feature votes")
      _ <- Either.cond(h.featureVotes.size <= MaxFeaturesInBlock, (), s"Block could not contain more than '$MaxFeaturesInBlock' feature votes")
      _ <- Either.cond(
        h.transactionCount == txs.size,
        (),
        s"Transaction count from block header '${h.transactionCount}' is not equal to transaction data size '${txs.size}' in block"
      )
      /* Check transaction count for genesis block on its creation. For mined blocks MiningConstraints is used */
      _ <- h.version match {
        case LegacyGenesisBlockVersion =>
          Either.cond(
            h.transactionCount <= MaxTransactionsPerBlockVer1Ver2,
            (),
            s"Too much transactions for genesis block: found '${h.transactionCount}', max count '$MaxTransactionsPerBlockVer1Ver2'"
          )
        case _ => Right(())
      }
    } yield Block(h, txs)).leftMap(GenericError(_))
  }

  def build(version: Byte,
            timestamp: Long,
            reference: ByteStr,
            consensusData: ConsensusBlockData,
            transactionData: Seq[Transaction],
            signerData: SignerData,
            featureVotes: Set[Short],
            genesisDataOpt: Option[GenesisData] = None): Either[GenericError, Block] = {
    build(BlockHeader(timestamp, version, reference, signerData, consensusData, featureVotes, genesisDataOpt, transactionData.size), transactionData)
  }

  def buildAndSign(version: Byte,
                   timestamp: Long,
                   reference: ByteStr,
                   consensusData: ConsensusBlockData,
                   transactionData: Seq[Transaction],
                   signer: PrivateKeyAccount,
                   featureVotes: Set[Short],
                   genesisDataOpt: Option[GenesisData] = None): Either[GenericError, Block] =
    build(version, timestamp, reference, consensusData, transactionData, SignerData(signer, ByteStr.empty), featureVotes, genesisDataOpt).right
      .map(unsigned =>
        unsigned.copy(blockHeader = unsigned.blockHeader.copy(signerData = SignerData(signer, ByteStr(crypto.sign(signer, unsigned.bytes()))))))

  def buildAndSignKeyBlock(timestamp: Long,
                           reference: ByteStr,
                           consensusData: ConsensusBlockData,
                           signer: PrivateKeyAccount,
                           featureVotes: Set[Short],
                           genesisDataOpt: Option[GenesisData] = None): Either[GenericError, Block] =
    buildAndSign(Block.NgBlockVersion, timestamp, reference, consensusData, Seq.empty, signer, featureVotes, genesisDataOpt)

  /**
    * Genesis transactions are:
    *   GenesisTransaction
    *   GenesisPermitTransaction
    *   GenesisRegisterNodeTransaction
    */
  def genesisTransactions(gs: GenesisSettings): Seq[Transaction] = gs match {
    case plain: PlainGenesisSettings => genesisTransactions(plain.transactions, plain.networkParticipants, plain.blockTimestamp)
    case _                           => Seq.empty
  }

  def genesisTransactions(txs: Seq[GenesisTransactionSettings],
                          participants: Seq[NetworkParticipantDescription],
                          blockTimestamp: Long): Seq[Transaction] = {
    val genesisAmountTxs = txs.map { ts =>
      GenesisTransaction.create(ts.recipient, ts.amount.units, blockTimestamp).explicitGet()
    }

    val timestampOffsetIterator = Stream.from(1).toIterator
    val additionalGenesisTxs = participants.flatMap {
      case NetworkParticipantDescription(pubKeyStr, roles) =>
        val participantKey = PublicKeyAccount.fromBase58String(pubKeyStr).explicitGet()

        val permitTxs = roles.map { role =>
          val txTimestamp = blockTimestamp - timestampOffsetIterator.next
          val parsedRole  = Role.fromStr(role).explicitGet()
          GenesisPermitTransaction.create(participantKey.toAddress, parsedRole, txTimestamp).explicitGet()
        }
        val registerNodeTx = GenesisRegisterNodeTransaction.create(participantKey, blockTimestamp).explicitGet()
        registerNodeTx +: permitTxs
    }

    genesisAmountTxs ++ additionalGenesisTxs
  }

  def genesis(genesisSettings: PlainGenesisSettings, consensusType: ConsensusType): Either[ValidationError, Block] = {
    val transactionGenesisData = genesisTransactions(genesisSettings)
    val consensusGenesisData = consensusType match {
      case ConsensusType.PoS =>
        PoSLikeConsensusBlockData(genesisSettings.initialBaseTarget, ByteStr(Array.fill(crypto.DigestSize)(0: Byte)))
      case ConsensusType.PoA =>
        PoALikeConsensusBlockData(overallSkippedRounds = 0)
      case ConsensusType.CFT =>
        CftLikeConsensusBlockData(votes = Seq.empty, overallSkippedRounds = 0)
    }
    val reference            = genesisReference()
    val genesisSignerPk      = Base58.decode(genesisSettings.genesisPublicKeyBase58).get
    val genesisSignerAccount = PublicKeyAccount(genesisSignerPk)
    val timestamp            = genesisSettings.blockTimestamp
    val signature            = genesisSettings.signature
    val blockVersion         = selectGenesisBlockVersion(genesisSettings)
    val genesisData          = GenesisData(genesisSettings.senderRoleEnabled)

    for {
      block <- Block.build(
        version = blockVersion,
        timestamp = timestamp,
        reference = reference,
        consensusData = consensusGenesisData,
        transactionData = transactionGenesisData,
        signerData = SignerData(genesisSignerAccount, signature.get),
        featureVotes = Set.empty,
        genesisDataOpt = Some(genesisData)
      )
      _ <- Either.cond(block.signatureValid(), (), GenericError("Passed genesis signature is not valid"))
    } yield block
  }

  def genesisReference(): ByteStr = ByteStr(Array.fill(SignatureLength)(-1: Byte))

  def selectGenesisBlockVersion(settings: PlainGenesisSettings): Byte = {
    selectGenesisBlockVersion(settings.version)
  }

  def selectGenesisBlockVersion(version: GenesisSettingsVersion): Byte = {
    version match {
      case GenesisSettingsVersion.LegacyVersion => LegacyGenesisBlockVersion
      case GenesisSettingsVersion.ModernVersion => ModernGenesisBlockVersion
    }
  }
}

object BlockFeeCalculator {

  val CurrentBlockFeePart: Fraction           = Fraction(2, 5)
  val CurrentBlockValidatorsFeePart: Fraction = Fraction(1, 4)

  /**
    * @return portfolios with 100% reward for miner and validators
    */
  def feesPortfolio(block: Block): Map[Address, Portfolio] = {
    val generator = block.signerData.generatorAddress
    Monoid[Map[Address, Portfolio]].combineAll {
      block.transactionData.map(blockTransactionFeeDiffs(_, generator))
    }
  }

  /**
    * @return portfolio with 60% reward for miner only
    */
  def prevBlockFeePart(block: Block): Portfolio = {
    val generator = block.signerData.generatorAddress
    Monoid[Map[Address, Portfolio]]
      .combineAll {
        block.transactionData.map(blockTransactionFeeDiffs(_, generator, Some(CurrentBlockFeePart)))
      }
      .get(generator)
      .orEmpty
  }

  private def blockTransactionFeeDiffs(tx: Transaction, generator: Address, excludingFraction: Option[Fraction] = None): Map[Address, Portfolio] = {
    tx match {
      case atx: AtomicTransaction =>
        Monoid[Map[Address, Portfolio]].combineAll {
          atx.transactions.map(blockTransactionFeeDiffs(_, generator))
        }
      case _ => innerBlockTransactionFeeDiffs(tx, generator, excludingFraction)
    }
  }

  private def innerBlockTransactionFeeDiffs(tx: Transaction, generator: Address, excludingFraction: Option[Fraction]): Map[Address, Portfolio] =
    tx match {
      case etxV2: ExecutedContractTransactionV2 if etxV2.validationProofs.nonEmpty =>
        val txFee              = etxV2.tx.fee
        val validatorAddresses = etxV2.validationProofs.view.map(_.validatorPublicKey.toAddress).toVector
        val perValidatorFee    = CurrentBlockValidatorsFeePart(txFee) / validatorAddresses.size
        val minerFee           = txFee - perValidatorFee * validatorAddresses.size

        val validatorPortfolios = validatorAddresses.view
          .map(_ -> blockTransactionFeeDiff(etxV2.tx.feeAssetId, perValidatorFee))
          .toMap

        val minerPortfolio = generator -> blockTransactionFeeDiff(etxV2.tx.feeAssetId, minerFee, excludingFraction)

        validatorPortfolios + minerPortfolio
      case etx: ExecutedContractTransaction =>
        Map(generator -> blockTransactionFeeDiff(etx.tx.feeAssetId, etx.tx.fee, excludingFraction))
      case _ =>
        Map(generator -> blockTransactionFeeDiff(tx.feeAssetId, tx.fee, excludingFraction))
    }

  private def blockTransactionFeeDiff(maybeAssetId: Option[AssetId], fee: Long, excludingFraction: Option[Fraction] = None): Portfolio = {
    val finalFee = excludingFraction.fold(fee)(f => fee - f(fee))
    maybeAssetId.fold {
      Portfolio(balance = finalFee, lease = LeaseBalance.empty, assets = Map.empty)
    } { assetId =>
      Portfolio(balance = 0, lease = LeaseBalance.empty, assets = Map(assetId -> finalFee))
    }
  }

  case class NgFee(nextBlockCarryFee: Long, portfolios: Map[Address, Portfolio])

  /**
    * @return 60% fee for the next round, 100% portfolios for validators and 40% portfolio for miner
    */
  def calcNgFee(blockchain: Blockchain, tx: Transaction, sponsorshipFeatureIsActive: Boolean, generator: Address): NgFee = {
    val portfolios = blockTransactionFeeDiffs(tx, generator).map {
      case (address, portfolio) if sponsorshipFeatureIsActive =>
        val assetFeeSum = portfolio.assets.map {
          case (assetId, assetFee) =>
            blockchain
              .assetDescription(assetId)
              .collect {
                case assetInfo if assetInfo.sponsorshipIsEnabled => Sponsorship.toWest(assetFee)
              }
              .getOrElse(0L)
        }.sum

        address -> Portfolio.empty.copy(balance = portfolio.balance + assetFeeSum)
      case portfolio => portfolio
    }

    val generatorPortfolio    = portfolios(generator)
    val currentBlockPortfolio = generatorPortfolio.multiply(BlockFeeCalculator.CurrentBlockFeePart)
    val nextBlockPortfolio    = generatorPortfolio.minus(currentBlockPortfolio)
    val finalPortfolios       = portfolios + (generator -> currentBlockPortfolio)
    NgFee(nextBlockPortfolio.balance, finalPortfolios)
  }
}
