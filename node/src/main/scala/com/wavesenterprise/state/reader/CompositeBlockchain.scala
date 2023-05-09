package com.wavesenterprise.state.reader

import cats.implicits._
import com.wavesenterprise.account.{Address, Alias, PublicKeyAccount}
import com.wavesenterprise.acl.{NonEmptyRole, OpType, Permissions}
import com.wavesenterprise.block.Block.BlockId
import com.wavesenterprise.block.{Block, BlockHeader}
import com.wavesenterprise.consensus._
import com.wavesenterprise.database.docker.KeysRequest
import com.wavesenterprise.docker.ContractInfo
import com.wavesenterprise.privacy.{PolicyDataHash, PolicyDataId}
import com.wavesenterprise.state.AssetHolder._
import com.wavesenterprise.state.ContractBlockchain.ContractReadingContext
import com.wavesenterprise.state._
import com.wavesenterprise.transaction.ValidationError.AliasDoesNotExist
import com.wavesenterprise.transaction.docker.{ExecutedContractData, ExecutedContractTransaction}
import com.wavesenterprise.transaction.smart.script.Script
import com.wavesenterprise.transaction.{AssetId, Transaction, ValidationError}
import com.wavesenterprise.utils.pki.CrlData
import org.apache.commons.codec.digest.DigestUtils

import java.security.cert.{Certificate, X509Certificate}

class CompositeBlockchain(inner: Blockchain, maybeDiff: Option[Diff], carry: Long = 0, contractValidatorsProvider: ContractValidatorsProvider)
    extends Blockchain {

  private def diff = maybeDiff.getOrElse(Diff.empty)

  override def addressPortfolio(address: Address): Portfolio =
    inner.addressPortfolio(address).combine(diff.portfolios.getOrElse(address.toAssetHolder, Portfolio.empty))

  override def contractPortfolio(contractId: ContractId): Portfolio =
    inner.contractPortfolio(contractId).combine(diff.portfolios.getOrElse(contractId.toAssetHolder, Portfolio.empty))

  override def addressBalance(address: Address, assetId: Option[AssetId]): Long =
    inner.addressBalance(address, assetId) + diff.portfolios.getOrElse(address.toAssetHolder, Portfolio.empty).balanceOf(assetId)

  override def contractBalance(contractId: ContractId, assetId: Option[AssetId], readingContext: ContractReadingContext): Long =
    inner.contractBalance(contractId, assetId, readingContext) + diff.portfolios
      .getOrElse(contractId.toAssetHolder, Portfolio.empty)
      .balanceOf(assetId)

  override def addressLeaseBalance(address: Address): LeaseBalance = {
    inner.addressLeaseBalance(address) |+| diff.portfolios.getOrElse(address.toAssetHolder, Portfolio.empty).lease
  }

  override def assetScript(id: ByteStr): Option[Script] = maybeDiff.flatMap(_.assetScripts.get(id)).getOrElse(inner.assetScript(id))

  override def hasAssetScript(id: ByteStr): Boolean = maybeDiff.flatMap(_.assetScripts.get(id)) match {
    case Some(s) => s.nonEmpty
    case None    => inner.hasAssetScript(id)
  }

  override def assets(): Set[AssetId] = inner.assets() ++ diff.assets.keySet

  override def asset(id: AssetId): Option[AssetInfo] = {
    diff.assets.get(id).orElse(inner.asset(id))
  }

  override def assetDescription(id: ByteStr): Option[AssetDescription] = {
    asset(id).map { assetInfo =>
      val script = assetScript(id)
      val sponsorshipIsEnabled = diff.sponsorship
        .get(id)
        .collect {
          case SponsorshipValue(isEnabled) => isEnabled
        }
        .getOrElse(inner.assetDescription(id).exists(_.sponsorshipIsEnabled))
      AssetDescription(assetInfo, script, sponsorshipIsEnabled)
    }
  }

  override def leaseDetails(leaseId: LeaseId): Option[LeaseDetails] = {
    inner.leaseDetails(leaseId).map { ld =>
      ld.copy(isActive = diff.leaseMap.get(leaseId).map(_.isActive).getOrElse(ld.isActive))
    } orElse diff.leaseMap.get(leaseId)
  }

  override def transactionInfo(id: ByteStr): Option[(Int, Transaction)] =
    diff.transactionsMap
      .get(id)
      .map(t => (t._1, t._2))
      .orElse(inner.transactionInfo(id))

  override def transactionHeight(id: ByteStr): Option[Int] =
    diff.transactionsMap
      .get(id)
      .map(_._1)
      .orElse(inner.transactionHeight(id))

  override def height: Int = inner.height + (if (maybeDiff.isDefined) 1 else 0)

  override def addressTransactions(address: Address,
                                   types: Set[Transaction.Type],
                                   count: Int,
                                   fromId: Option[ByteStr]): Either[String, Seq[(Int, Transaction)]] =
    addressTransactionsFromStateAndDiff(inner, maybeDiff)(address, types, count, fromId)

  override def resolveAlias(alias: Alias): Either[ValidationError, Address] = inner.resolveAlias(alias) match {
    case Right(addr) => Right(diff.aliases.getOrElse(alias, addr))
    case Left(_)     => diff.aliases.get(alias).toRight(AliasDoesNotExist(alias))
  }

  override def collectAddressLposPortfolios[A](pf: PartialFunction[(Address, Portfolio), A]): Map[Address, A] = {
    val b = Map.newBuilder[Address, A]
    for ((a, p) <- diff.portfolios.collectAddresses if p.lease != LeaseBalance.empty || p.balance != 0) {
      pf.runWith(b += a -> _)(a -> this.westPortfolio(a))
    }

    inner.collectAddressLposPortfolios(pf) ++ b.result()
  }

  override def containsTransaction(tx: Transaction): Boolean = diff.transactionsMap.contains(tx.id()) || inner.containsTransaction(tx)

  override def containsTransaction(id: ByteStr): Boolean = diff.transactionsMap.contains(id) || inner.containsTransaction(id)

  override def filledVolumeAndFee(orderId: ByteStr): VolumeAndFee =
    diff.orderFills.get(orderId).orEmpty.combine(inner.filledVolumeAndFee(orderId))

  override def addressBalanceSnapshots(address: Address, from: Int, to: Int): Seq[BalanceSnapshot] = {
    if (to <= inner.height || maybeDiff.isEmpty) {
      inner.addressBalanceSnapshots(address, from, to)
    } else {
      val bs = BalanceSnapshot(height, addressPortfolio(address))
      if (inner.height > 0 && from < this.height) bs +: inner.addressBalanceSnapshots(address, from, to) else Seq(bs)
    }
  }

  override def contractBalanceSnapshots(contractId: ContractId, from: Int, to: Int): Seq[BalanceSnapshot] = {
    if (to <= inner.height || maybeDiff.isEmpty) {
      inner.contractBalanceSnapshots(contractId, from, to)
    } else {
      val bs = BalanceSnapshot(height, contractPortfolio(contractId))
      if (inner.height > 0 && from < this.height) bs +: inner.contractBalanceSnapshots(contractId, from, to) else Seq(bs)
    }
  }

  override def accounts(): Set[Address] = inner.accounts() ++ diff.addresses

  override def accountScript(address: Address): Option[Script] = {
    diff.scripts.get(address) match {
      case None            => inner.accountScript(address)
      case Some(None)      => None
      case Some(Some(scr)) => Some(scr)
    }
  }

  override def hasScript(address: Address): Boolean = {
    diff.scripts.get(address) match {
      case None          => inner.hasScript(address)
      case Some(None)    => false
      case Some(Some(_)) => true
    }
  }

  def accountDataSlice(acc: Address, from: Int, to: Int): AccountDataInfo = {
    val fromInner = inner.accountDataSlice(acc, from, to)
    val fromDiff  = diff.accountData.get(acc).orEmpty
    fromInner.combine(fromDiff)
  }

  override def accountData(acc: Address): AccountDataInfo = {
    val fromInner = inner.accountData(acc)
    val fromDiff  = diff.accountData.get(acc).orEmpty
    fromInner.combine(fromDiff)
  }

  override def accountData(acc: Address, key: String): Option[DataEntry[_]] = {
    val diffData = diff.accountData.get(acc).orEmpty
    diffData.data.get(key).orElse(inner.accountData(acc, key))
  }

  private def changedBalances(pred: Portfolio => Boolean, f: Address => Long): Map[Address, Long] =
    for {
      (address, p) <- diff.portfolios.collectAddresses
      if pred(p)
    } yield address -> f(address)

  override def addressAssetDistribution(assetId: ByteStr): AssetDistribution = {
    val fromInner = inner.addressAssetDistribution(assetId)
    val fromDiff  = AssetDistribution(changedBalances(_.assets.getOrElse(assetId, 0L) != 0, addressBalance(_, Some(assetId))))

    fromInner |+| fromDiff
  }

  override def addressAssetDistributionAtHeight(assetId: AssetId,
                                                height: Int,
                                                count: Int,
                                                fromAddress: Option[Address]): Either[ValidationError, AssetDistributionPage] = {
    inner.addressAssetDistributionAtHeight(assetId, height, count, fromAddress)
  }

  override def addressWestDistribution(height: Int): Map[Address, Long] = {
    val innerDistribution = inner.addressWestDistribution(height)
    if (height < this.height) innerDistribution
    else {
      innerDistribution ++ changedBalances(_.balance != 0, addressBalance(_))
    }
  }

  override def score: BigInt = inner.score

  override def scoreOf(blockId: ByteStr): Option[BigInt] = inner.scoreOf(blockId)

  override def blockHeaderAndSize(height: Int): Option[(BlockHeader, Int)] = inner.blockHeaderAndSize(height)

  override def blockHeaderAndSize(blockId: ByteStr): Option[(BlockHeader, Int)] = inner.blockHeaderAndSize(blockId)

  override def lastBlock: Option[Block] = inner.lastBlock

  override def lastPersistenceBlock: Option[Block] = inner.lastPersistenceBlock

  override def carryFee: Long = carry

  override def blockBytes(height: Int): Option[Array[Transaction.Type]] = inner.blockBytes(height)

  override def blockBytes(blockId: ByteStr): Option[Array[Transaction.Type]] = inner.blockBytes(blockId)

  override def heightOf(blockId: ByteStr): Option[Int] = inner.heightOf(blockId)

  /** Returns the most recent block IDs, starting from the most recent  one */
  override def lastBlockIds(howMany: Int): Seq[ByteStr] = inner.lastBlockIds(howMany)

  /** Returns the most recent block IDs, starting from the startBlock */
  def lastBlockIds(startBlock: BlockId, howMany: Int): Option[Seq[BlockId]] = inner.lastBlockIds(startBlock, howMany)

  /** Returns a chain of blocks starting with the block with the given ID (from oldest to newest) */
  override def blockIdsAfter(parentSignature: ByteStr, howMany: Int): Option[Seq[ByteStr]] = inner.blockIdsAfter(parentSignature, howMany)

  override def parent(block: Block, back: Int): Option[Block] = inner.parent(block, back)

  override def parentHeader(block: Block): Option[BlockHeader] = inner.parentHeader(block)

  /** Features related */
  override def approvedFeatures: Map[Short, Int] = inner.approvedFeatures

  override def activatedFeatures: Map[Short, Int] = inner.activatedFeatures

  override def featureVotes(height: Int): Map[Short, Int] = inner.featureVotes(height)

  override def append(
      diff: Diff,
      carryFee: Long,
      block: Block,
      consensusPostActionDiff: ConsensusPostActionDiff,
      certificates: Set[X509Certificate]
  ): Unit = inner.append(diff, carryFee, block, consensusPostActionDiff, certificates)

  override def rollbackTo(targetBlockId: ByteStr): Either[String, Seq[Block]] = inner.rollbackTo(targetBlockId)

  override def permissions(acc: Address): Permissions = {
    val in                  = inner.permissions(acc)
    val diffed: Permissions = diff.permissions.getOrElse(acc, Permissions.empty)
    in combine diffed
  }

  override def miners: MinerQueue =
    inner.miners.update(diff.permissions)

  override def contractValidators: ContractValidatorPool =
    inner.contractValidators.update(diff.permissions)

  override def lastBlockContractValidators: Set[Address] = contractValidatorsProvider.get()

  override def minerBanHistory(address: Address): MinerBanHistory =
    inner.minerBanHistory(address)

  override def bannedMiners(height: Int): Seq[Address] =
    inner.bannedMiners(height)

  override def warningsAndBans: Map[Address, Seq[MinerBanlistEntry]] = {
    inner.warningsAndBans
  }

  override def participantPubKey(address: Address): Option[PublicKeyAccount] = {
    diff.registrations.find(_.address == address) match {
      case Some(ParticipantRegistration(_, pubKey, regOperation)) =>
        regOperation match {
          case OpType.Add    => Some(pubKey)
          case OpType.Remove => None
        }
      case None => inner.participantPubKey(address)
    }
  }

  override def networkParticipants(): Seq[Address] = {
    val (addingReg, excludingReg) = diff.registrations.partition(_.opType == OpType.Add)
    inner.networkParticipants().diff(excludingReg.map(_.address)).union(addingReg.map(_.address))
  }

  override def policies(): Set[ByteStr] = inner.policies() ++ diff.policies.keySet

  override def policyExists(policyId: ByteStr): Boolean = {
    diff.policies.contains(policyId) || inner.policyExists(policyId)
  }

  override def policyOwners(policyId: ByteStr): Set[Address] = {
    val fromBlockchain = inner.policyOwners(policyId)
    diff.policies.get(policyId) match {
      case Some(policy: PolicyDiffValue) => fromBlockchain ++ policy.ownersToAdd -- policy.ownersToRemove
      case _                             => fromBlockchain
    }
  }

  override def policyRecipients(policyId: ByteStr): Set[Address] = {
    val fromBlockchain = inner.policyRecipients(policyId)
    diff.policies.get(policyId) match {
      case Some(policy: PolicyDiffValue) => fromBlockchain ++ policy.recipientsToAdd -- policy.recipientsToRemove
      case _                             => fromBlockchain
    }
  }

  override def contracts(): Set[ContractInfo] = {
    val fromInner = inner.contracts().map(c => c.contractId -> c).toMap
    val fromDiff  = diff.contracts
    (fromInner ++ fromDiff).values.toSet
  }

  override def contract(contractId: ContractId): Option[ContractInfo] = diff.contracts.get(contractId).orElse(inner.contract(contractId))

  override def contractKeys(keysRequest: KeysRequest, readingContext: ContractReadingContext): Vector[String] = {
    val contractId   = keysRequest.contractId
    val keysFromDiff = diff.contractsData.get(contractId).map(_.data.keys).getOrElse(Iterable.empty)
    inner.contractKeys(keysRequest.copy(knownKeys = keysFromDiff), readingContext)
  }

  override def contractData(contractId: ByteStr, keys: Iterable[String], readingContext: ContractReadingContext): ExecutedContractData = {
    val fromInner = inner.contractData(contractId, keys, readingContext)
    val fromDiff  = diff.contractsData.get(contractId).map(_.filterKeys(keys.toSet)).orEmpty
    fromInner.combine(fromDiff)
  }

  override def contractData(contractId: ByteStr, readingContext: ContractReadingContext): ExecutedContractData = {
    val fromInner = inner.contractData(contractId, readingContext)
    val fromDiff  = diff.contractsData.get(contractId).orEmpty
    fromInner.combine(fromDiff)
  }

  override def contractData(contractId: ByteStr, key: String, readingContext: ContractReadingContext): Option[DataEntry[_]] = {
    val diffData = diff.contractsData.get(contractId).orEmpty
    diffData.data.get(key).orElse(inner.contractData(contractId, key, readingContext))
  }

  override def executedTxFor(forTxId: AssetId): Option[ExecutedContractTransaction] = {
    diff.executedTxMapping
      .get(forTxId)
      .flatMap(transactionInfo)
      .map(_._2)
      .map(_.asInstanceOf[ExecutedContractTransaction])
      .orElse(inner.executedTxFor(forTxId))
  }

  override def hasExecutedTxFor(forTxId: ByteStr): Boolean = {
    diff.executedTxMapping.contains(forTxId) || inner.hasExecutedTxFor(forTxId)
  }

  override def policyDataHashes(policyId: ByteStr): Set[PolicyDataHash] = {
    val fromDiff  = diff.policiesDataHashes.mapValues(_.map(_.dataHash)).getOrElse(policyId, Set.empty)
    val fromInner = inner.policyDataHashes(policyId)
    fromInner.combine(fromDiff)
  }

  override def policyDataHashExists(policyId: ByteStr, dataHash: PolicyDataHash): Boolean = {
    policyDataHashes(policyId).contains(dataHash)
  }

  override def allNonEmptyRoleAddresses: Stream[Address] = {
    val fromDiff = diff.permissions.collect {
      case (address, permissions) if permissions.exists(_.role.isInstanceOf[NonEmptyRole]) =>
        address
    }.toSet

    val fromInner = inner.allNonEmptyRoleAddresses.filterNot(fromDiff.contains)

    fromDiff.toStream ++ fromInner
  }

  override def policyDataHashTxId(id: PolicyDataId): Option[ByteStr] = {
    diff.policiesDataHashes
      .get(id.policyId)
      .flatMap { txs =>
        txs.collectFirst {
          case tx if tx.dataHash == id.dataHash => tx.id()
        }
      }
      .orElse {
        inner.policyDataHashTxId(id)
      }
  }

  override def certByDistinguishedNameHash(dnHash: String): Option[Certificate] = {
    val dnHashByteStr = ByteStr(dnHash.toArray.map(_.toByte))
    diff.certByDnHash
      .get(dnHashByteStr)
      .orElse(inner.certByDistinguishedNameHash(dnHash))
  }

  override def certByDistinguishedName(distinguishedName: String): Option[Certificate] = {
    val dnHash = ByteStr(DigestUtils.sha1(distinguishedName))

    diff.certByDnHash
      .get(dnHash)
      .orElse(inner.certByDistinguishedName(distinguishedName))
  }

  override def certByPublicKey(publicKey: PublicKeyAccount): Option[Certificate] = {
    diff.certDnHashByPublicKey
      .get(publicKey)
      .flatMap(diff.certByDnHash.get)
      .orElse(inner.certByPublicKey(publicKey))
  }

  override def certByFingerPrint(fingerprint: ByteStr): Option[Certificate] = {
    diff.certDnHashByFingerprint
      .get(fingerprint)
      .flatMap(diff.certByDnHash.get)
      .orElse(inner.certByFingerPrint(fingerprint))
  }

  override def certsAtHeight(height: Int): Set[Certificate] = {
    if (height < this.height)
      inner.certsAtHeight(height)
    else
      diff.certByDnHash.values.toSet
  }

  override def aliasesIssuedByAddress(address: Address): Set[Alias] = {
    val aliasesFromDiff = diff.aliases.toList
      .groupBy { case (_, address) => address }
      .get(address)
      .map(_.map { case (alias, _) => alias })
      .toList
      .flatten
      .toSet
    aliasesFromDiff ++ inner.aliasesIssuedByAddress(address)
  }

  override def crlDataByHash(crlHash: ByteStr): Option[CrlData] = {
    diff.crlDataByHash.get(crlHash).orElse(inner.crlDataByHash(crlHash))
  }

  override def actualCrls(issuer: PublicKeyAccount, timestamp: Long): Set[CrlData] = {
    diff.crlHashesByIssuer // diff contains all the data for a new CDP, no need to go to the DB in that case
      .get(issuer)
      .map(_.map(diff.crlDataByHash(_)))
      .getOrElse(inner.actualCrls(issuer, timestamp))
  }
}

object CompositeBlockchain {

  def composite(inner: Blockchain, diff: Option[Diff]): CompositeBlockchain =
    new CompositeBlockchain(inner, diff, contractValidatorsProvider = new ContractValidatorsProvider(inner))

  def composite(inner: Blockchain, diff: Diff, carryFee: Long, contractValidatorsProvider: ContractValidatorsProvider): CompositeBlockchain =
    new CompositeBlockchain(inner, Some(diff), carryFee, contractValidatorsProvider)

  def composite(inner: Blockchain, diff: Diff, carryFee: Long = 0): CompositeBlockchain =
    new CompositeBlockchain(inner, Some(diff), carryFee, new ContractValidatorsProvider(inner))
}
