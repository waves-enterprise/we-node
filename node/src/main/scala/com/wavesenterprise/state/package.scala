package com.wavesenterprise

import cats.kernel.Monoid
import com.wavesenterprise.account.{Address, AddressOrAlias, Alias}
import com.wavesenterprise.block.{Block, BlockHeader}
import com.wavesenterprise.state.ContractBlockchain.ContractReadingContext
import com.wavesenterprise.transaction.ValidationError.AliasDoesNotExist
import com.wavesenterprise.utils.EitherUtils.EitherExt
import com.wavesenterprise.transaction._
import com.wavesenterprise.transaction.lease.LeaseTransaction
import com.wavesenterprise.utils.Paged
import play.api.libs.json._
import supertagged.TaggedType

import scala.reflect.ClassTag
import scala.util.Try

package object state {

  implicit class IntExtension(val x: Int) extends AnyVal {
    def positiveOrZero: Int = x.max(0)
  }

  def safeSum(x: Long, y: Long): Long = Try(Math.addExact(x, y)).getOrElse(Long.MinValue)

  // common logic for addressTransactions method of BlockchainUpdaterImpl and CompositeBlockchain
  def addressTransactionsFromStateAndDiff(b: Blockchain, d: Option[Diff])(address: Address,
                                                                          txTypes: Set[Transaction.Type],
                                                                          count: Int,
                                                                          fromId: Option[ByteStr]): Either[String, Seq[(Int, Transaction)]] = {
    /* delete later */
    def collectAddresses(s: Seq[(Int, Transaction, Set[AssetHolder])]): Seq[(Int, Transaction, Set[Address])] = s.collect {
      case (i, transaction, holders) => (i, transaction, holders.collectAddresses.toSet)
    }

    def transactionsFromDiff(d: Diff): Seq[(Int, Transaction, Set[AssetHolder])] = d.transactionsMap.values.view.toSeq.reverse

    def withPagination(s: Seq[(Int, Transaction, Set[Address])]): Seq[(Int, Transaction, Set[Address])] =
      fromId match {
        case None     => s
        case Some(id) => s.dropWhile(_._2.id() != id).drop(1)
      }

    def withFilterAndLimit(txs: Seq[(Int, Transaction, Set[Address])]): Seq[(Int, Transaction)] =
      txs
        .collect {
          case (height, tx, addresses) if addresses(address) && (txTypes.isEmpty || txTypes.contains(tx.builder.typeId)) => (height, tx)
        }
        .take(count)

    def txsFromBlockchain(s: Seq[(Int, Transaction)]): Either[String, Seq[(Int, Transaction)]] =
      s.length match {
        case `count`        => Right(s)
        case l if l < count => b.addressTransactions(address, txTypes, count - l, None).map(s ++ _)
        case _              => Right(s.take(count))
      }

    def transactions: Diff => Either[String, Seq[(Int, Transaction)]] =
      txsFromBlockchain _ compose withFilterAndLimit compose withPagination compose collectAddresses compose transactionsFromDiff

    d.fold(b.addressTransactions(address, txTypes, count, fromId)) { diff =>
      fromId match {
        case Some(id) if !diff.transactionsMap.contains(id) =>
          b.addressTransactions(address, txTypes, count, fromId)
        case _ => transactions(diff)
      }
    }
  }

  implicit class EitherValidationErrorExt[L <: ValidationError, R](ei: Either[L, R]) {
    def liftValidationError[T <: Transaction](t: T): Either[ValidationError, R] = {
      ei.left.map(e => ValidationError.GenericError(e.toString))
    }
  }

  implicit class Cast[A](a: A) {
    def cast[B: ClassTag]: Option[B] = {
      a match {
        case b: B => Some(b)
        case _    => None
      }
    }
  }

  implicit class BlockchainExt(blockchain: Blockchain) {
    def isEmpty: Boolean = blockchain.height == 0

    def contains(block: Block): Boolean       = blockchain.contains(block.uniqueId)
    def contains(signature: ByteStr): Boolean = blockchain.heightOf(signature).isDefined

    def blockById(blockId: ByteStr): Option[Block] = blockchain.blockBytes(blockId).map(parseBlockBytes)
    def blockAt(height: Int): Option[Block]        = blockchain.blockBytes(height).map(parseBlockBytes)

    def blockHeaderAt(height: Int): Option[BlockHeader] = blockchain.blockHeaderAndSize(height).map(_._1)

    private[this] def parseBlockBytes(bb: Array[Byte]): Block =
      Block.parseBytes(bb).fold(e => throw new RuntimeException("Can't parse block bytes", e), identity)

    def lastBlockId: Option[ByteStr] = blockchain.lastBlock.map(_.uniqueId)

    def lastBlockTimestamp: Option[Long] = blockchain.lastBlock.map(_.timestamp)

    def lastBlocks(howMany: Int): Seq[Block] = {
      (Math.max(1, blockchain.height - howMany + 1) to blockchain.height).flatMap(blockchain.blockAt).reverse
    }

    def genesis: Block = blockchain.blockAt(1).get
    def resolveAlias(aoa: AddressOrAlias): Either[ValidationError, Address] =
      aoa match {
        case a: Address => Right(a)
        case a: Alias   => blockchain.resolveAlias(a)
      }

    def canCreateAlias(alias: Alias): Boolean = blockchain.resolveAlias(alias) match {
      case Left(AliasDoesNotExist(_)) => true
      case _                          => false
    }

    //todo: investigate me, why we use `balanceSnapshots` here? Is it real 'effectiveBalance'?
    def effectiveBalance(address: Address, atHeight: Int, confirmations: Int): Long = {
      val bottomLimit = (atHeight - confirmations + 1).max(1).min(atHeight)
      val balances    = blockchain.addressBalanceSnapshots(address, bottomLimit, atHeight)
      if (balances.isEmpty) 0L else balances.view.map(_.effectiveBalance).min
    }

    def activeLeases(address: Address): Seq[(Int, LeaseTransaction)] =
      blockchain
        .addressTransactions(address, Set(LeaseTransaction.typeId), Int.MaxValue, None)
        .explicitGet()
        .collect { case (h, l: LeaseTransaction) if blockchain.leaseDetails(l.id()).exists(_.isActive) => h -> l }

    def unsafeHeightOf(id: ByteStr): Int =
      blockchain
        .heightOf(id)
        .getOrElse(throw new IllegalStateException(s"Can't find a block: $id"))

    def westPortfolio(address: Address): Portfolio = Portfolio(
      balance = blockchain.addressBalance(address),
      lease = blockchain.addressLeaseBalance(address),
      assets = Map.empty
    )

    def assetHolderSpendableBalance(assetHolder: AssetHolder): Long = assetHolder match {
      case Account(address) =>
        val westBalance  = blockchain.addressBalance(address)
        val leaseBalance = blockchain.addressLeaseBalance(address)
        westBalance - leaseBalance.out
      case Contract(contractId) =>
        blockchain.contractBalance(contractId, None, ContractReadingContext.Default)
    }
  }

  object AssetDistribution extends TaggedType[Map[Address, Long]]
  type AssetDistribution = AssetDistribution.Type

  implicit val dstMonoid: Monoid[AssetDistribution] = new Monoid[AssetDistribution] {
    override def empty: AssetDistribution = AssetDistribution(Map.empty[Address, Long])

    override def combine(x: AssetDistribution, y: AssetDistribution): AssetDistribution = {
      AssetDistribution(x ++ y)
    }
  }

  implicit val dstWrites: Writes[AssetDistribution] = Writes { dst =>
    Json
      .toJson(dst.map {
        case (addr, balance) => addr.stringRepr -> balance
      })
  }

  object AssetDistributionPage extends TaggedType[Paged[Address, AssetDistribution]]
  type AssetDistributionPage = AssetDistributionPage.Type

  implicit val dstPageWrites: Writes[AssetDistributionPage] = Writes { page =>
    JsObject(
      Map(
        "hasNext"  -> JsBoolean(page.hasNext),
        "lastItem" -> Json.toJson(page.lastItem.map(_.stringRepr)),
        "items"    -> Json.toJson(page.items)
      )
    )
  }

}
