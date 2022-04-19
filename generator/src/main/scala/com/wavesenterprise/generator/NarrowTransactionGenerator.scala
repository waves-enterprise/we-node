package com.wavesenterprise.generator

import cats.Show
import com.wavesenterprise.account.{AddressScheme, Alias, PrivateKeyAccount}
import com.wavesenterprise.generator.NarrowTransactionGenerator.Settings
import com.wavesenterprise.generator.transaction.{DataTransactionGenerator, TransactionTypesSettings}
import com.wavesenterprise.generator.utils.Gen._
import com.wavesenterprise.settings.FeeSettings.FeesEnabled
import com.wavesenterprise.transaction._
import com.wavesenterprise.transaction.acl.PermitTransactionV1
import com.wavesenterprise.transaction.assets._
import com.wavesenterprise.transaction.assets.exchange._
import com.wavesenterprise.transaction.lease.{LeaseCancelTransaction, LeaseCancelTransactionV2, LeaseTransaction, LeaseTransactionV2}
import com.wavesenterprise.transaction.transfer.{ParsedTransfer, _}
import com.wavesenterprise.transaction.validation.TransferValidation
import com.wavesenterprise.utils.EitherUtils.EitherExt
import com.wavesenterprise.utils.LoggerFacade
import org.slf4j.LoggerFactory

import java.util.concurrent.ThreadLocalRandom
import scala.concurrent.duration._
import scala.util.Random

class NarrowTransactionGenerator(settings: Settings, val accounts: Seq[PrivateKeyAccount], fees: FeesEnabled) extends TransactionGenerator {

  private def r = ThreadLocalRandom.current

  private val chainId = AddressScheme.getAddressSchema.chainId

  private val log     = LoggerFacade(LoggerFactory.getLogger(getClass))
  private val typeGen = DistributedRandomGenerator(settings.probabilities)

  private val dataTransactionGenerator = new DataTransactionGenerator(settings.transactionTypesSettings.dataTransaction, fees)

  /**
    * Fees taken from mainnet config
    *   in WEST, decimal points = 8
    */
  private val transferTxFee             = fees.forTxType(TransferTransaction.typeId)
  private val massTransferBaseFee       = fees.forTxType(MassTransferTransaction.typeId)
  private val massTransferAdditionalFee = fees.forTxTypeAdditional(MassTransferTransaction.typeId)
  private val issueTxFee                = fees.forTxType(IssueTransaction.typeId)
  private val exchangeTxFee             = fees.forTxType(ExchangeTransaction.typeId)
  private val leaseTxFee                = fees.forTxType(LeaseTransaction.typeId)
  private val leaseCancelTxFee          = fees.forTxType(LeaseCancelTransaction.typeId)
  private val reissueTxFee              = fees.forTxType(ReissueTransaction.typeId)
  private val burnTxFee                 = fees.forTxType(BurnTransaction.typeId)
  private val createAliasTxFee          = fees.forTxType(CreateAliasTransaction.typeId)
  private val sponsorFeeTxFee           = fees.forTxType(SponsorFeeTransactionV1.typeId)

  private def logOption[T <: Transaction](txE: Either[ValidationError, T])(implicit m: Manifest[T]): Option[T] = {
    txE match {
      case Left(e) =>
        log.warn(s"${m.runtimeClass.getName}: ${e.toString}")
        None
      case Right(tx) => Some(tx)
    }
  }

  override def next(): Iterator[Transaction] = generate(settings.transactions).toIterator

  def generate(n: Int): Seq[Transaction] = {
    val issueTransactionSender = randomFrom(accounts).get

    val tradeAssetIssue = IssueTransactionV2
      .selfSigned(
        chainId,
        issueTransactionSender,
        "TRADE".getBytes("UTF-8"),
        "WE DEX is the best exchange ever".getBytes("UTF-8"),
        100000000,
        2,
        reissuable = false,
        issueTxFee + r.nextInt(100000000),
        System.currentTimeMillis(),
        script = None
      )
      .right
      .get

    val now = System.currentTimeMillis()

    val generated = (0 until (n * 1.2).toInt).foldLeft(
      (
        Seq.empty[Transaction],
        Seq.empty[IssueTransactionV2],
        Seq.empty[IssueTransactionV2],
        Seq.empty[LeaseTransactionV2],
        Seq.empty[CreateAliasTransaction]
      )) {
      case ((allTxsWithValid, validIssueTxs, reissuableIssueTxs, activeLeaseTransactions, aliases), i) =>
        val exchangeTxFeeGen    = (exchangeTxFee + r.nextInt(100000)) * 3
        val leaseTxFeeGen       = (leaseTxFee + r.nextInt(100000)) * 3
        val leaseCancelTxFeeGen = (leaseCancelTxFee + r.nextInt(100000)) * 3

        val ts = now + i

        val tx = typeGen.getRandom match {
          case IssueTransactionV2 =>
            val sender      = randomFrom(accounts).get
            val name        = new Array[Byte](10)
            val description = new Array[Byte](10)
            r.nextBytes(name)
            r.nextBytes(description)
            val reissuable = r.nextBoolean()
            val amount     = 100000000L + Random.nextInt(Int.MaxValue)
            logOption(
              IssueTransactionV2
                .selfSigned(chainId,
                            sender,
                            name,
                            description,
                            amount,
                            Random.nextInt(9).toByte,
                            reissuable,
                            issueTxFee + r.nextInt(100000000),
                            ts,
                            None))
          case TransferTransactionV2 =>
            val useAlias  = r.nextBoolean()
            val recipient = if (useAlias && aliases.nonEmpty) randomFrom(aliases).map(_.alias).get else randomFrom(accounts).get.toAddress
            val sendAsset = r.nextBoolean()
            val senderAndAssetOpt = if (sendAsset) {
              val asset = randomFrom(validIssueTxs)
              asset.map(issue => {
                val pk = accounts.find(_ == issue.sender).get
                (pk, Some(issue.id()))
              })
            } else Some((randomFrom(accounts).get, None))
            senderAndAssetOpt.flatMap {
              case (sender, asset) =>
                logOption(
                  TransferTransactionV2
                    .selfSigned(sender,
                                asset,
                                None,
                                ts,
                                500,
                                transferTxFee + r.nextInt(10000),
                                recipient,
                                Array.fill(r.nextInt(100))(r.nextInt().toByte)))
            }
          case ReissueTransactionV2 =>
            val reissuable = r.nextBoolean()
            randomFrom(reissuableIssueTxs).flatMap(assetTx => {
              val sender = accounts.find(_.address == assetTx.sender.address).get
              logOption(ReissueTransactionV2.selfSigned(chainId, sender, assetTx.id(), Random.nextInt(Int.MaxValue), reissuable, reissueTxFee, ts))
            })
          case BurnTransactionV2 =>
            randomFrom(validIssueTxs).flatMap(assetTx => {
              val sender = accounts.find(_.address == assetTx.sender.address).get
              logOption(BurnTransactionV2.selfSigned(chainId, sender, assetTx.id(), Random.nextInt(1000), burnTxFee, ts))
            })
          case ExchangeTransactionV2 =>
            val matcher = randomFrom(accounts).get
            val seller  = randomFrom(accounts).get
            val pair    = AssetPair(None, Some(tradeAssetIssue.id()))
            // XXX generate order version
            val sellOrder = OrderV1.sell(seller, matcher, pair, 100000000, 1, ts, ts + 30.days.toMillis, exchangeTxFeeGen)
            val buyer     = randomFrom(accounts).get
            val buyOrder  = OrderV1.buy(buyer, matcher, pair, 100000000, 1, ts, ts + 1.day.toMillis, exchangeTxFeeGen)
            logOption(ExchangeTransactionV2.create(matcher, buyOrder, sellOrder, 100000000, 1, 300000, 300000, exchangeTxFeeGen, ts))
          case LeaseTransactionV2 =>
            val sender   = randomFrom(accounts).get
            val useAlias = r.nextBoolean()
            val recipientOpt =
              if (useAlias && aliases.nonEmpty) randomFrom(aliases.filter(_.sender != sender)).map(_.alias)
              else randomFrom(accounts.filter(_ != sender).map(_.toAddress))
            recipientOpt.flatMap(recipient => logOption(LeaseTransactionV2.selfSigned(None, sender, recipient, 1, leaseTxFeeGen, ts)))
          case LeaseCancelTransactionV2 =>
            randomFrom(activeLeaseTransactions).flatMap(lease => {
              val sender = accounts.find(_.address == lease.sender.address).get
              logOption(LeaseCancelTransactionV2.selfSigned(chainId, sender, leaseCancelTxFeeGen, ts, lease.id()))
            })
          case CreateAliasTransactionV2 =>
            val sender      = randomFrom(accounts).get
            val aliasString = NarrowTransactionGenerator.generateAlias()
            logOption(CreateAliasTransactionV2.selfSigned(sender, Alias.buildWithCurrentChainId(aliasString).explicitGet(), createAliasTxFee, ts))
          case PermitTransactionV1 =>
            val sender     = randomFrom(accounts).get
            val targetAddr = randomFrom(accounts).get.toAddress
            val timestamp  = ts
            val permOp     = genPermissionOp(timestamp).get
            logOption(PermitTransactionV1.selfSigned(sender, targetAddr, timestamp, 100000, permOp))
          case MassTransferTransactionV1 =>
            val transferCount = r.nextInt(TransferValidation.MaxTransferCount)
            val transfers = for (i <- 0 to transferCount) yield {
              val useAlias  = r.nextBoolean()
              val recipient = if (useAlias && aliases.nonEmpty) randomFrom(aliases).map(_.alias).get else randomFrom(accounts).get.toAddress
              val amount    = r.nextLong(500000)
              ParsedTransfer(recipient, amount)
            }
            val sendAsset = r.nextBoolean()
            val senderAndAssetOpt = if (sendAsset) {
              val asset = randomFrom(validIssueTxs)
              asset.map(issue => {
                val pk = accounts.find(_ == issue.sender).get
                (pk, Some(issue.id()))
              })
            } else Some((randomFrom(accounts).get, None))
            senderAndAssetOpt.flatMap {
              case (sender, asset) =>
                logOption(
                  MassTransferTransactionV1.selfSigned(sender,
                                                       asset,
                                                       transfers.toList,
                                                       ts,
                                                       massTransferBaseFee + massTransferAdditionalFee * transferCount,
                                                       Array.fill(r.nextInt(100))(r.nextInt().toByte)))
            }
          case DataTransactionV1 =>
            val sender = randomFrom(accounts).get
            logOption(dataTransactionGenerator.generateTxV1(sender, ts))
          case DataTransactionV2 =>
            val sender = randomFrom(accounts).get
            logOption(dataTransactionGenerator.generateTxV2(sender, ts))
          case SponsorFeeTransactionV1 =>
            randomFrom(validIssueTxs).flatMap(assetTx => {
              val sender = accounts.find(_.address == assetTx.sender.address).get
              logOption(SponsorFeeTransactionV1.selfSigned(sender, assetTx.id(), isEnabled = true, fee = sponsorFeeTxFee, timestamp = ts))
            })
        }

        (tx.map(tx => allTxsWithValid :+ tx).getOrElse(allTxsWithValid), tx match {
          case Some(tx: IssueTransactionV2) => validIssueTxs :+ tx
          case _                            => validIssueTxs
        }, tx match {
          case Some(tx: IssueTransactionV2) if tx.reissuable  => reissuableIssueTxs :+ tx
          case Some(tx: ReissueTransaction) if !tx.reissuable => reissuableIssueTxs.filter(_.id() != tx.id())
          case _                                              => reissuableIssueTxs
        }, tx match {
          case Some(tx: LeaseTransactionV2)     => activeLeaseTransactions :+ tx
          case Some(tx: LeaseCancelTransaction) => activeLeaseTransactions.filter(_.id() != tx.leaseId)
          case _                                => activeLeaseTransactions
        }, tx match {
          case Some(tx: CreateAliasTransaction) => aliases :+ tx
          case _                                => aliases
        })
    }

    generated._1.take(n)
  }
}

object NarrowTransactionGenerator {

  final case class ScriptSettings(dappAccount: String, paymentAssets: Set[String], functions: Seq[ScriptSettings.Function])
  object ScriptSettings {
    final case class Function(name: String, args: Seq[Function.Arg])
    object Function {
      final case class Arg(`type`: String, value: String)
    }
  }

  final case class Settings(transactions: Int,
                            probabilities: Map[TransactionParser, Double],
                            transactionTypesSettings: TransactionTypesSettings,
                            scripts: Seq[ScriptSettings])

  private val minAliasLength = 4
  private val maxAliasLength = 30
  private val aliasAlphabet  = "-.0123456789@_abcdefghijklmnopqrstuvwxyz".toVector

  def generateAlias(): String = {
    val len = Random.nextInt(maxAliasLength - minAliasLength) + minAliasLength
    Random.shuffle(aliasAlphabet).take(len).mkString
  }

  object Settings {
    implicit val toPrintable: Show[Settings] = { x =>
      import x._
      s"""transactions per iteration: $transactions
         |frequencies:
         |  ${probabilities.mkString("\n  ")}""".stripMargin
    }
  }

}
