package com.wavesenterprise.state

import cats.Semigroup
import cats.implicits._
import cats.kernel.Monoid
import com.wavesenterprise.account.{Address, Alias, PublicKeyAccount}
import com.wavesenterprise.acl.{OpType, Permissions}
import com.wavesenterprise.certs.CertChain
import com.wavesenterprise.docker.ContractInfo
import com.wavesenterprise.privacy.PolicyDataHash
import com.wavesenterprise.transaction._
import com.wavesenterprise.transaction.docker.ExecutedContractData
import com.wavesenterprise.transaction.smart.script.Script
import com.wavesenterprise.utils.pki.{CrlCollection, CrlData}
import org.apache.commons.codec.digest.DigestUtils
import play.api.libs.json.{JsObject, JsString, JsValue}

import java.security.cert.X509Certificate

/**
  * Amounts of WEST leased TO (`in` field) and FROM (`out` field) the entity
  */
case class LeaseBalance(in: Long, out: Long) {
  def isEmpty: Boolean  = in == 0L && out == 0L
  def nonEmpty: Boolean = !isEmpty

  override def toString: String = {
    s"LeaseBalance(in = $in, out = $out)"
  }
}

object LeaseBalance {
  val empty: LeaseBalance = LeaseBalance(0, 0)

  implicit val m: Monoid[LeaseBalance] = new Monoid[LeaseBalance] {
    override def empty: LeaseBalance = LeaseBalance.empty

    override def combine(x: LeaseBalance, y: LeaseBalance): LeaseBalance =
      LeaseBalance(safeSum(x.in, y.in), safeSum(x.out, y.out))
  }
}

case class VolumeAndFee(volume: Long, fee: Long)

object VolumeAndFee {
  val empty: VolumeAndFee = VolumeAndFee(0, 0)

  implicit val m: Monoid[VolumeAndFee] = new Monoid[VolumeAndFee] {
    override def empty: VolumeAndFee = VolumeAndFee.empty

    override def combine(x: VolumeAndFee, y: VolumeAndFee): VolumeAndFee =
      VolumeAndFee(x.volume + y.volume, x.fee + y.fee)
  }
}

case class AssetInfo(issuer: AssetHolder,
                     height: Int,
                     timestamp: Long,
                     name: String,
                     description: String,
                     decimals: Byte,
                     reissuable: Boolean,
                     volume: BigInt)

case class AssetDescription(issuer: AssetHolder,
                            height: Int,
                            timestamp: Long,
                            name: String,
                            description: String,
                            decimals: Byte,
                            reissuable: Boolean,
                            totalVolume: BigInt,
                            script: Option[Script],
                            sponsorshipIsEnabled: Boolean)

object AssetDescription {
  def apply(asset: AssetInfo, script: Option[Script], sponsorshipIsEnabled: Boolean): AssetDescription = {
    AssetDescription(
      asset.issuer,
      asset.height,
      asset.timestamp,
      asset.name,
      asset.description,
      asset.decimals,
      asset.reissuable,
      asset.volume,
      script,
      sponsorshipIsEnabled
    )
  }
}

case class AccountDataInfo(data: Map[String, DataEntry[_]])

object AccountDataInfo {
  implicit val accountDataInfoMonoid: Monoid[AccountDataInfo] = new Monoid[AccountDataInfo] {
    override def empty: AccountDataInfo = AccountDataInfo(Map.empty)

    override def combine(x: AccountDataInfo, y: AccountDataInfo): AccountDataInfo = AccountDataInfo(x.data ++ y.data)
  }
}

sealed trait Sponsorship
case class SponsorshipValue(isEnabled: Boolean) extends Sponsorship
case object SponsorshipNoInfo                   extends Sponsorship

object Sponsorship {

  implicit val sponsorshipMonoid: Monoid[Sponsorship] = new Monoid[Sponsorship] {
    override def empty: Sponsorship = SponsorshipNoInfo

    override def combine(x: Sponsorship, y: Sponsorship): Sponsorship = y match {
      case SponsorshipNoInfo => x
      case _                 => y
    }
  }

  def toWest(assetFee: Long, baseUnitRatio: Long = 1): Long = {
    baseUnitRatio.ensuring(_ > 0, "BaseUnitRatio must be positive")
    val westAmount = BigInt(assetFee) / BigInt(baseUnitRatio)
    westAmount.ensuring(_.isValidLong, s"West amount overflow '$westAmount'").toLong
  }

  def fromWest(westFee: Long, baseUnitRatio: Long = 1): Long = {
    baseUnitRatio.ensuring(_ > 0, "BaseUnitRatio must be positive")
    val assetAmount = BigInt(westFee) * BigInt(baseUnitRatio)
    assetAmount.ensuring(_.isValidLong, s"Asset amount overflow '$assetAmount'").toLong
  }
}

sealed abstract class PolicyDiff
case object EmptyPolicyDiff extends PolicyDiff
case class PolicyDiffValue(ownersToAdd: Set[Address], recipientsToAdd: Set[Address], ownersToRemove: Set[Address], recipientsToRemove: Set[Address])
    extends PolicyDiff {
  def negate(): PolicyDiffValue = PolicyDiffValue(ownersToRemove, recipientsToRemove, ownersToAdd, recipientsToAdd)
}
object PolicyDiffValue {
  def fromTx(tx: UpdatePolicyTransaction): PolicyDiffValue = {
    tx.opType match {
      case OpType.Add    => PolicyDiffValue(tx.owners.toSet, tx.recipients.toSet, Set.empty, Set.empty)
      case OpType.Remove => PolicyDiffValue(Set.empty, Set.empty, tx.owners.toSet, tx.recipients.toSet)
    }
  }

  def fromTx(tx: CreatePolicyTransaction): PolicyDiffValue = {
    PolicyDiffValue(tx.owners.toSet, tx.recipients.toSet, Set.empty, Set.empty)
  }
}

object PolicyDiff {
  implicit val policyMonoid: Monoid[PolicyDiff] = new Monoid[PolicyDiff] {
    override def empty: PolicyDiff = EmptyPolicyDiff

    override def combine(first: PolicyDiff, second: PolicyDiff): PolicyDiff = (first, second) match {
      case (EmptyPolicyDiff, EmptyPolicyDiff)       => EmptyPolicyDiff
      case (_: PolicyDiffValue, EmptyPolicyDiff)    => first
      case (EmptyPolicyDiff, _: PolicyDiffValue)    => second
      case (f: PolicyDiffValue, s: PolicyDiffValue) => combineTwoPolicies(f, s)
    }

    private def combineTwoPolicies(first: PolicyDiffValue, second: PolicyDiffValue): PolicyDiff = {
      val ownersToAdd        = (first.ownersToAdd -- second.ownersToRemove) ++ second.ownersToAdd
      val recipientsToAdd    = (first.recipientsToAdd -- second.recipientsToRemove) ++ second.recipientsToAdd
      val ownersToRemove     = (first.ownersToRemove -- second.ownersToAdd) ++ second.ownersToRemove
      val recipientsToRemove = (first.recipientsToRemove -- second.recipientsToAdd) ++ second.recipientsToRemove

      val ownersIntersect     = ownersToAdd.intersect(ownersToRemove)
      val recipientsIntersect = recipientsToAdd.intersect(recipientsToRemove)

      PolicyDiffValue(
        ownersToAdd -- ownersIntersect,
        recipientsToAdd -- recipientsIntersect,
        ownersToRemove -- ownersIntersect,
        recipientsToRemove -- recipientsIntersect
      )
    }

  }
}

case class ParticipantRegistration(address: Address, pubKey: PublicKeyAccount, opType: OpType)

sealed trait AssetHolder
case class Account(address: Address) extends AssetHolder
object Account {
  val binaryHeader: Byte = 0x00.toByte
}
case class Contract(contractId: ContractId) extends AssetHolder
object Contract {
  val binaryHeader: Byte = 0x01.toByte
}

case class ContractId(byteStr: ByteStr) {
  def toAssetHolder: AssetHolder = Contract(this)

  override def toString: String = byteStr.toString()
}

object AssetHolder {

  implicit class AssetHolderExt(val assetHolder: AssetHolder) extends AnyVal {

    def toJson: JsValue = {
      assetHolder match {
        case Account(address) =>
          JsObject(
            Seq(
              "type"    -> JsString("Account"),
              "address" -> JsString(address.stringRepr)
            ))
        case Contract(contractId) =>
          JsObject(
            Seq(
              "type"       -> JsString("Contract"),
              "contractId" -> JsString(contractId.byteStr.base58)
            ))
      }
    }

    @inline
    final def product[T](forAccount: Address => T, forContract: ContractId => T): T = assetHolder match {
      case Account(address)     => forAccount(address)
      case Contract(contractId) => forContract(contractId)
    }
  }

  implicit class AssetHolderMapExt[T](val self: Map[AssetHolder, T]) extends AnyVal {
    def collectAddresses: Map[Address, T] =
      self.collect {
        case (ba: Account, value) => ba.address -> value
      }

    def collectContractIds: Map[ContractId, T] =
      self.collect {
        case (bc: Contract, value) => bc.contractId -> value
      }
  }

  implicit class AssetHolderIterableExt(val self: Iterable[AssetHolder]) extends AnyVal {
    def collectAddresses: Iterable[Address] =
      self.collect {
        case ba: Account => ba.address
      }

    def collectContractIds: Iterable[ContractId] =
      self.collect {
        case bc: Contract => bc.contractId
      }
  }

  implicit class AddressExt(val self: Address) extends AnyVal {
    def toAssetHolder: AssetHolder = Account(self)
  }

  implicit class AddressMapExt[T](val self: Map[Address, T]) extends AnyVal {
    def toAssetHolderMap: Map[AssetHolder, T] =
      self.map { case (k: Address, v) => (Account(k), v) }
  }

  implicit class ContractIdMapExt[T](val self: Map[ContractId, T]) extends AnyVal {
    def toAssetHolderMap: Map[AssetHolder, T] =
      self.map { case (k: ContractId, v) => (Contract(k), v) }
  }
}

case class Diff(transactions: List[Transaction],
                transactionsMap: Map[ByteStr, (Int, Transaction, Set[AssetHolder])],
                portfolios: Map[AssetHolder, Portfolio],
                assets: Map[AssetId, AssetInfo],
                aliases: Map[Alias, Address],
                orderFills: Map[ByteStr, VolumeAndFee],
                leaseState: Map[ByteStr, Boolean],
                scripts: Map[Address, Option[Script]],
                assetScripts: Map[AssetId, Option[Script]],
                accountData: Map[Address, AccountDataInfo],
                sponsorship: Map[AssetId, Sponsorship],
                registrations: Seq[ParticipantRegistration],
                permissions: Map[Address, Permissions],
                contracts: Map[ContractId, ContractInfo],
                contractsData: Map[ByteStr, ExecutedContractData],
                executedTxMapping: Map[ByteStr, ByteStr],
                policies: Map[ByteStr, PolicyDiff],
                policiesDataHashes: Map[ByteStr, Set[PolicyDataHashTransaction]],
                dataHashToSender: Map[PolicyDataHash, Address],
                certByDnHash: Map[ByteStr, X509Certificate],
                certDnHashByDistinguishedName: Map[String, ByteStr],
                certDnHashByPublicKey: Map[PublicKeyAccount, ByteStr],
                certDnHashByFingerprint: Map[ByteStr, ByteStr],
                crlDataByHash: Map[ByteStr, CrlData],
                crlHashesByIssuer: Map[PublicKeyAccount, Set[ByteStr]],
                usedCrlHashes: Set[ByteStr]) {

  lazy val assetHolderTransactionIds: Map[AssetHolder, List[(Int, ByteStr)]] = {
    val map: List[(AssetHolder, Set[(Int, Byte, Long, ByteStr)])] = transactionsMap.toList
      .flatMap { case (id, (h, tx, accs)) => accs.map(acc => acc -> Set((h, tx.builder.typeId, tx.timestamp, id))) }
    val groupedByAcc = map.foldLeft(Map.empty[AssetHolder, Set[(Int, Byte, Long, ByteStr)]]) {
      case (m, (acc, set)) =>
        m.combine(Map(acc -> set))
    }
    groupedByAcc
      .mapValues(l => l.toList.sortBy { case (h, _, t, _) => (-h, -t) }) // fresh head ([h=2, h=1, h=0])
      .mapValues(_.map({ case (_, typ, _, id) => (typ.toInt, id) }))
  }

  lazy val addresses: Seq[Address] = (portfolios.collectAddresses.keys ++ permissions.keys ++ registrations.map(_.address)).toSeq.distinct

  lazy val contractIds: Seq[ContractId] = portfolios.collectContractIds.keys.toSeq
}

object Diff {

  def apply(height: Int,
            tx: Transaction,
            portfolios: Map[AssetHolder, Portfolio] = Map.empty,
            assets: Map[AssetId, AssetInfo] = Map.empty,
            aliases: Map[Alias, Address] = Map.empty,
            orderFills: Map[ByteStr, VolumeAndFee] = Map.empty,
            leaseState: Map[ByteStr, Boolean] = Map.empty,
            scripts: Map[Address, Option[Script]] = Map.empty,
            assetScripts: Map[AssetId, Option[Script]] = Map.empty,
            accountData: Map[Address, AccountDataInfo] = Map.empty,
            permissions: Map[Address, Permissions] = Map.empty,
            registrations: Seq[ParticipantRegistration] = Seq.empty,
            sponsorship: Map[AssetId, Sponsorship] = Map.empty,
            contracts: Map[ContractId, ContractInfo] = Map.empty,
            contractsData: Map[ByteStr, ExecutedContractData] = Map.empty,
            executedTxMapping: Map[ByteStr, ByteStr] = Map.empty,
            policies: Map[ByteStr, PolicyDiff] = Map.empty,
            policiesDataHashes: Map[ByteStr, Set[PolicyDataHashTransaction]] = Map.empty,
            dataHashToSender: Map[PolicyDataHash, Address] = Map.empty,
            certs: Map[ByteStr, X509Certificate] = Map.empty,
            certDnHashByDistinguishedName: Map[String, ByteStr] = Map.empty,
            certDnHashByPublicKey: Map[PublicKeyAccount, ByteStr] = Map.empty,
            certDnHashByFingerprint: Map[ByteStr, ByteStr] = Map.empty,
            crlDataByHash: Map[ByteStr, CrlData] = Map.empty,
            crlHashesByIssuer: Map[PublicKeyAccount, Set[ByteStr]] = Map.empty,
            usedCrlHashes: Set[ByteStr] = Set.empty): Diff =
    Diff(
      transactions = List(tx),
      transactionsMap = Map((tx.id(), (height, tx, portfolios.keys.toSet))),
      portfolios = portfolios,
      assets = assets,
      aliases = aliases,
      orderFills = orderFills,
      leaseState = leaseState,
      scripts = scripts,
      assetScripts = assetScripts,
      accountData = accountData,
      sponsorship = sponsorship,
      permissions = permissions,
      registrations = registrations,
      contracts = contracts,
      contractsData = contractsData,
      executedTxMapping = executedTxMapping,
      policies = policies,
      policiesDataHashes = policiesDataHashes,
      dataHashToSender = dataHashToSender,
      certByDnHash = certs,
      certDnHashByDistinguishedName = certDnHashByDistinguishedName,
      certDnHashByPublicKey = certDnHashByPublicKey,
      certDnHashByFingerprint = certDnHashByFingerprint,
      crlDataByHash = crlDataByHash,
      crlHashesByIssuer = crlHashesByIssuer,
      usedCrlHashes = usedCrlHashes
    )

  val empty =
    new Diff(
      transactions = List.empty,
      transactionsMap = Map.empty,
      portfolios = Map.empty,
      assets = Map.empty,
      aliases = Map.empty,
      orderFills = Map.empty,
      leaseState = Map.empty,
      scripts = Map.empty,
      assetScripts = Map.empty,
      accountData = Map.empty,
      sponsorship = Map.empty,
      registrations = Seq.empty,
      permissions = Map.empty,
      contracts = Map.empty,
      contractsData = Map.empty,
      executedTxMapping = Map.empty,
      policies = Map.empty,
      policiesDataHashes = Map.empty,
      dataHashToSender = Map.empty,
      certByDnHash = Map.empty,
      certDnHashByDistinguishedName = Map.empty,
      certDnHashByPublicKey = Map.empty,
      certDnHashByFingerprint = Map.empty,
      crlDataByHash = Map.empty,
      crlHashesByIssuer = Map.empty,
      usedCrlHashes = Set.empty
    )

  def fromCertChain(certChain: CertChain, crlCollection: CrlCollection): Either[ValidationError, Diff] = {
    (certChain.caCert :: certChain.userCert :: certChain.intermediateCerts.toList)
      .foldM((Map.empty[ByteStr, X509Certificate], Map.empty[String, ByteStr], Map.empty[PublicKeyAccount, ByteStr], Map.empty[ByteStr, ByteStr])) {
        case ((certs, certDnHashByDistinguishedName, certDnHashByPublicKey, certDnHashByFingerprint), cert) =>
          PublicKeyAccount.fromBytes(cert.getPublicKey.getEncoded).leftMap(ValidationError.fromCryptoError).map { pka =>
            val dn          = cert.getSubjectX500Principal.getName
            val dnHash      = ByteStr(DigestUtils.sha1(dn))
            val fingerprint = ByteStr(DigestUtils.sha1(cert.getEncoded))
            (certs + (dnHash                        -> cert),
             certDnHashByDistinguishedName + (dn    -> dnHash),
             certDnHashByPublicKey + (pka           -> dnHash),
             certDnHashByFingerprint + (fingerprint -> dnHash))
          }
      }
      .map {
        case (certByDnHash, certDnHashByDistinguishedName, certDnHashByPublicKey, certDnHashByFingerprint) =>
          val crlDataByHash = crlCollection.newCrlData.groupBy(_.crlHash()).mapValues(_.head)
          val crlHashesByIssuer = crlDataByHash.toList
            .groupBy {
              case (_, crlData) => crlData.issuer
            }
            .mapValues(_.map {
              case (hash, _) => hash
            }.toSet)
          empty.copy(
            certByDnHash = certByDnHash,
            certDnHashByDistinguishedName = certDnHashByDistinguishedName,
            certDnHashByPublicKey = certDnHashByPublicKey,
            certDnHashByFingerprint = certDnHashByFingerprint,
            crlDataByHash = crlDataByHash,
            crlHashesByIssuer = crlHashesByIssuer,
            usedCrlHashes = crlCollection.crls.map(CrlData.hashForCrl)
          )
      }
  }

  private def groupParticipantsByAddress(regs: Seq[ParticipantRegistration]): Map[Address, ParticipantRegistration] = {
    regs.map(p => p.address -> p).toMap
  }

  implicit val participantsReqSemigroup: Semigroup[Seq[ParticipantRegistration]] =
    (old: Seq[ParticipantRegistration], newer: Seq[ParticipantRegistration]) => {
      val oldMap = groupParticipantsByAddress(old)
      val newMap = groupParticipantsByAddress(newer)

      val crossingAddressesResult = oldMap.flatMap {
        case (addr, oldParReg) =>
          newMap.get(addr) match {
            case None => Some(oldParReg)
            case Some(newParReg) =>
              (oldParReg.opType, newParReg.opType) match {
                case (OpType.Add, OpType.Remove) => None
                case (OpType.Remove, OpType.Add) => None
                case _                           => Some(newParReg)
              }
          }
      }.toSeq

      val newWithoutOld = newMap.filterKeys(addr => !oldMap.contains(addr))
      crossingAddressesResult ++ newWithoutOld.values
    }

  implicit val diffMonoid: Monoid[Diff] = new Monoid[Diff] {
    override def empty: Diff = Diff.empty

    override def combine(older: Diff, newer: Diff): Diff =
      Diff(
        transactions = older.transactions ++ newer.transactions,
        transactionsMap = older.transactionsMap ++ newer.transactionsMap,
        portfolios = older.portfolios.combine(newer.portfolios),
        assets = older.assets ++ newer.assets,
        aliases = older.aliases ++ newer.aliases,
        orderFills = older.orderFills.combine(newer.orderFills),
        leaseState = older.leaseState ++ newer.leaseState,
        scripts = older.scripts ++ newer.scripts,
        assetScripts = older.assetScripts ++ newer.assetScripts,
        accountData = older.accountData.combine(newer.accountData),
        permissions = older.permissions.combine(newer.permissions),
        registrations = older.registrations.combine(newer.registrations),
        sponsorship = older.sponsorship.combine(newer.sponsorship),
        contracts = older.contracts ++ newer.contracts,
        contractsData = older.contractsData.combine(newer.contractsData),
        executedTxMapping = older.executedTxMapping ++ newer.executedTxMapping,
        policies = older.policies.combine(newer.policies),
        policiesDataHashes = older.policiesDataHashes.combine(newer.policiesDataHashes),
        dataHashToSender = older.dataHashToSender ++ newer.dataHashToSender,
        certByDnHash = older.certByDnHash ++ newer.certByDnHash,
        certDnHashByDistinguishedName = older.certDnHashByDistinguishedName ++ newer.certDnHashByDistinguishedName,
        certDnHashByPublicKey = older.certDnHashByPublicKey ++ newer.certDnHashByPublicKey,
        certDnHashByFingerprint = older.certDnHashByFingerprint ++ newer.certDnHashByFingerprint,
        crlDataByHash = older.crlDataByHash ++ newer.crlDataByHash,
        crlHashesByIssuer = older.crlHashesByIssuer.combine(newer.crlHashesByIssuer),
        usedCrlHashes = older.usedCrlHashes ++ newer.usedCrlHashes
      )
  }

  def recombine[A](seq: Seq[A])(implicit ev: Semigroup[Seq[A]]): Seq[A] = {
    seq.foldLeft(Seq.empty[A]) {
      case (combinedRegs, reg) =>
        ev.combine(combinedRegs, Seq(reg))
    }
  }

  def feeAssetIdPortfolio(tx: Transaction, sender: AssetHolder, blockchain: Blockchain): Map[AssetHolder, Portfolio] =
    tx.feeAssetId match {
      case None => Map(sender -> Portfolio(-tx.fee, LeaseBalance.empty, Map.empty))
      case Some(feeAssetId) =>
        val senderPortfolio = Map(sender -> Portfolio(0, LeaseBalance.empty, Map(feeAssetId -> -tx.fee)))
        val sponsorPortfolio = blockchain
          .assetDescription(feeAssetId)
          .collect {
            case desc if desc.sponsorshipIsEnabled =>
              val feeInWest = Sponsorship.toWest(tx.fee)
              Map(desc.issuer -> Portfolio(-feeInWest, LeaseBalance.empty, Map(feeAssetId -> tx.fee)))
          }
          .getOrElse(Map.empty)
        senderPortfolio.combine(sponsorPortfolio)
    }
}
