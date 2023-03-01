package com.wavesenterprise

import com.wavesenterprise.TransactionGen._
import com.wavesenterprise.account.PublicKeyAccount._
import com.wavesenterprise.account._
import com.wavesenterprise.acl.{OpType, PermissionsGen}
import com.wavesenterprise.block.Block
import com.wavesenterprise.lagonaki.mocks.TestBlock
import com.wavesenterprise.lang.v1.compiler.Terms._
import com.wavesenterprise.network.{
  GotDataResponse,
  GotEncryptedDataResponse,
  NoDataResponse,
  NodeAttributes,
  PrivateDataRequest,
  PrivateDataResponse
}
import com.wavesenterprise.privacy.{PolicyDataHash, PolicyDataId, PolicyMetaData}
import com.wavesenterprise.settings.{NodeMode, TestFees}
import com.wavesenterprise.state._
import com.wavesenterprise.state.diffs.ENOUGH_AMT
import com.wavesenterprise.transaction._
import com.wavesenterprise.transaction.assets._
import com.wavesenterprise.transaction.assets.exchange._
import com.wavesenterprise.transaction.lease._
import com.wavesenterprise.transaction.smart.script.Script
import com.wavesenterprise.transaction.smart.script.v1.ScriptV1
import com.wavesenterprise.transaction.smart.{SetScriptTransaction, SetScriptTransactionV1, SetScriptValidation}
import com.wavesenterprise.transaction.transfer._
import com.wavesenterprise.utils.EitherUtils.EitherExt
import org.scalacheck.{Arbitrary, Gen}
import org.scalatest.Suite

import java.nio.charset.StandardCharsets
import scala.concurrent.duration._

trait TransactionGen extends CoreTransactionGen { _: Suite =>

  def severalPrivKeysGenerator(min: Int = 2, max: Int = 20): Gen[List[PrivateKeyAccount]] = {
    severalGenerators(accountGen, min, max)
  }

  def otherAccountGen(candidate: PrivateKeyAccount): Gen[PrivateKeyAccount] = accountGen.flatMap(Gen.oneOf(candidate, _))

  def selfSignedSetScriptTransactionGenP(sender: PrivateKeyAccount, s: Script): Gen[SetScriptTransaction] =
    for {
      fee         <- smallFeeGen
      timestamp   <- timestampGen
      name        <- genBoundedString(1, SetScriptValidation.MaxNameSize)
      description <- genBoundedString(1, 1024) // to fit in BaseGlobal#MaxBase64String size for base64 presentation of tx
    } yield SetScriptTransactionV1
      .selfSigned(currentChainId, sender, Some(s), name, description, fee, timestamp)
      .explicitGet()

  def leaseAndCancelGeneratorP(leaseSender: PrivateKeyAccount,
                               recipient: AddressOrAlias,
                               unleaseSender: PrivateKeyAccount): Gen[(LeaseTransaction, LeaseCancelTransaction)] =
    for {
      (_, amount, fee, timestamp, _) <- leaseParamGen
      (lease, _)                     <- createLease(leaseSender, amount, fee, timestamp, recipient, None)
      fee2                           <- smallFeeGen
      (unlease, _)                   <- createLeaseCancel(unleaseSender, lease.id(), fee2, timestamp + 1, None)
    } yield (lease, unlease)

  val twoLeasesGen: Gen[(LeaseTransaction, LeaseTransaction)] = for {
    (sender, amount, fee, timestamp, recipient) <- leaseParamGen
    amount2                                     <- positiveLongGen
    recipient2                                  <- accountOrAliasGen
    fee2                                        <- smallFeeGen
    (lease1, _)                                 <- createLease(sender, amount, fee, timestamp, recipient, None)
    (lease2, _)                                 <- createLease(sender, amount2, fee2, timestamp + 1, recipient2, None)
  } yield (lease1, lease2)

  val leaseAndCancelWithOtherSenderGen: Gen[(LeaseTransaction, LeaseCancelTransaction)] = for {
    (sender, amount, fee, timestamp, recipient) <- leaseParamGen
    otherSender: PrivateKeyAccount              <- accountGen
    (lease, _)                                  <- createLease(sender, amount, fee, timestamp, recipient, None)
    fee2                                        <- smallFeeGen
    timestamp2                                  <- positiveLongGen
    (leaseCancel, _)                            <- createLeaseCancel(otherSender, lease.id(), fee2, timestamp2, None)
  } yield (lease, leaseCancel)

  def transferGeneratorP(sender: PrivateKeyAccount,
                         recipient: AddressOrAlias,
                         assetId: Option[AssetId],
                         feeAssetId: Option[AssetId]): Gen[TransferTransactionV2] =
    for {
      (_, _, _, amount, timestamp, _, feeAmount, attachment) <- transferParamGen
    } yield TransferTransactionV2.selfSigned(sender, assetId, feeAssetId, timestamp, amount, feeAmount, recipient, attachment).explicitGet()

  def versionedTransferGeneratorP(sender: PrivateKeyAccount,
                                  recipient: AddressOrAlias,
                                  assetId: Option[AssetId],
                                  feeAssetId: Option[AssetId]): Gen[TransferTransactionV2] =
    for {
      (_, _, _, amount, timestamp, _, feeAmount, attachment) <- transferParamGen
    } yield TransferTransactionV2
      .selfSigned(sender, assetId, feeAssetId, timestamp, amount, feeAmount, recipient, attachment)
      .explicitGet()

  def transferGeneratorPV2(timestamp: Long, sender: PrivateKeyAccount, recipient: AddressOrAlias, maxAmount: Long): Gen[TransferTransactionV2] =
    for {
      amount                                    <- Gen.choose(1, maxAmount)
      (_, _, _, _, _, _, feeAmount, attachment) <- transferParamGen
    } yield TransferTransactionV2.selfSigned(sender, None, None, timestamp, amount, feeAmount, recipient, attachment).explicitGet()

  def transferGeneratorPV3(timestamp: Long,
                           sender: PrivateKeyAccount,
                           recipient: AddressOrAlias,
                           maxAmount: Long,
                           trustedAddress: Option[Address]): Gen[TransferTransactionV3] =
    for {
      amount                                    <- Gen.choose(1, maxAmount)
      (_, _, _, _, _, _, feeAmount, attachment) <- transferParamGen
      maybeAtomicBadge = trustedAddress.map(address => AtomicBadge(Some(address)))
    } yield TransferTransactionV3.selfSigned(sender, None, None, timestamp, amount, feeAmount, recipient, attachment, maybeAtomicBadge).explicitGet()

  def transferGeneratorP(timestamp: Long,
                         sender: PrivateKeyAccount,
                         recipient: AddressOrAlias,
                         assetId: Option[AssetId],
                         feeAssetId: Option[AssetId]): Gen[TransferTransactionV2] =
    for {
      (_, _, _, amount, _, _, feeAmount, attachment) <- transferParamGen
    } yield TransferTransactionV2.selfSigned(sender, assetId, feeAssetId, timestamp, amount, feeAmount, recipient, attachment).explicitGet()

  def westTransferGeneratorP(sender: PrivateKeyAccount, recipient: AddressOrAlias): Gen[TransferTransactionV2] =
    transferGeneratorP(sender, recipient, None, None)

  def westTransferGeneratorP(timestamp: Long, sender: PrivateKeyAccount, recipient: AddressOrAlias): Gen[TransferTransactionV2] =
    transferGeneratorP(timestamp, sender, recipient, None, None)

  def massTransferV1GeneratorP(sender: PrivateKeyAccount, transfers: List[ParsedTransfer], assetId: Option[AssetId]): Gen[MassTransferTransactionV1] =
    for {
      (_, _, _, _, timestamp, _, feeAmount, attachment) <- transferParamGen
    } yield MassTransferTransactionV1.selfSigned(sender, assetId, transfers, timestamp, feeAmount, attachment).explicitGet()

  def massTransferV2GeneratorP(sender: PrivateKeyAccount,
                               transfers: List[ParsedTransfer],
                               assetId: Option[AssetId],
                               feeAssetId: Option[AssetId]): Gen[MassTransferTransactionV2] =
    for {
      (_, _, _, _, timestamp, _, feeAmount, attachment) <- transferParamGen
    } yield MassTransferTransactionV2.selfSigned(sender, assetId, transfers, timestamp, feeAmount, attachment, feeAssetId).explicitGet()

  def versionedTransferGenP(sender: PublicKeyAccount, recipient: Address, proofs: Proofs) =
    (for {
      amt       <- positiveLongGen
      fee       <- smallFeeGen
      timestamp <- timestampGen
    } yield TransferTransactionV2.create(sender, None, None, timestamp, amt, fee, recipient, Array.emptyByteArray, proofs).explicitGet())
      .label("VersionedTransferTransactionP")

  val cancelFeeSponsorshipGen = for {
    sender           <- accountGen
    (_, _, _, tx, _) <- sponsorFeeCancelSponsorFeeGen(sender)
  } yield {
    tx
  }

  val orderV1Gen: Gen[Order] = for {
    (sender, matcher, pair, orderType, price, amount, timestamp, expiration, matcherFee) <- orderParamGen
  } yield Order(sender, matcher, pair, orderType, price, amount, timestamp, expiration, matcherFee, 1: Byte)

  val orderV2Gen: Gen[Order] = for {
    (sender, matcher, pair, orderType, amount, price, timestamp, expiration, matcherFee) <- orderParamGen
  } yield Order(sender, matcher, pair, orderType, amount, price, timestamp, expiration, matcherFee, 2: Byte)

  val orderGen: Gen[Order] = Gen.oneOf(orderV1Gen, orderV2Gen)

  val arbitraryOrderGen: Gen[Order] = for {
    (sender, matcher, pair, orderType, _, _, _, _, _) <- orderParamGen
    amount                                            <- Arbitrary.arbitrary[Long]
    price                                             <- Arbitrary.arbitrary[Long]
    timestamp                                         <- Arbitrary.arbitrary[Long]
    expiration                                        <- Arbitrary.arbitrary[Long]
    matcherFee                                        <- Arbitrary.arbitrary[Long]
  } yield Order(sender, matcher, pair, orderType, amount, price, timestamp, expiration, matcherFee, 1: Byte)

  val randomTransactionGen = (for {
    tr              <- transferV2Gen
    (is, ri, bu, _) <- issueReissueBurnGen
    ca              <- createAliasV2Gen
    tx              <- Gen.oneOf(tr, is, ri, ca, bu)
  } yield tx).label("random transaction")

  def randomTransactionsGen(count: Int) =
    for {
      transactions <- Gen.listOfN(count, randomTransactionGen)
    } yield transactions

  val genesisGen: Gen[GenesisTransaction] = addressGen.flatMap(genesisGeneratorP)

  def genesisGeneratorP(recipient: Address): Gen[GenesisTransaction] =
    for {
      amt <- Gen.choose(1, 100000000L * 100000000L)
      ts  <- positiveIntGen
    } yield GenesisTransaction.create(recipient, amt, ts).explicitGet()

  def dataTransactionV1GenP(sender: PrivateKeyAccount, data: List[DataEntry[_]]): Gen[DataTransactionV1] =
    (for {
      timestamp <- timestampGen
    } yield DataTransactionV1.selfSigned(sender, sender, data, timestamp, 15000000).explicitGet())
      .label("DataTransactionV1P")

  def dataTransactionV2GenP(sender: PrivateKeyAccount, data: List[DataEntry[_]], feeAssetId: Option[AssetId] = None): Gen[DataTransactionV2] =
    (for {
      timestamp <- ntpTimestampGen
    } yield DataTransactionV2.selfSigned(sender, sender, data, timestamp, 15000000, feeAssetId).explicitGet())
      .label("DataTransactionV2P")

  def preconditionsTransferAndLease(typed: EXPR): Gen[(GenesisTransaction, SetScriptTransaction, LeaseTransaction, TransferTransactionV2)] =
    for {
      master    <- accountGen
      recipient <- addressGen
      ts        <- positiveIntGen
      genesis = GenesisTransaction.create(master.toAddress, ENOUGH_AMT, ts).explicitGet()
      setScript <- selfSignedSetScriptTransactionGenP(master, ScriptV1(typed).explicitGet())
      transfer  <- transferGeneratorPV2(ts, master, recipient, ENOUGH_AMT / 2)
      fee       <- smallFeeGen
      lease = LeaseTransactionV2
        .selfSigned(None, master, recipient, ENOUGH_AMT / 2, fee, ts)
        .explicitGet()
    } yield (genesis, setScript, lease, transfer)

  def updatePolicyTransactionV1GenWithRecipients(policyOwner: PrivateKeyAccount,
                                                 policyIdGen: Gen[Array[Byte]] = genBoundedString(10, 100),
                                                 recipientsToAdd: List[Address],
                                                 recipientsToRemove: List[Address],
                                                 ownersToRemove: List[Address],
                                                 ownersMaxSize: Int = 30): Gen[UpdatePolicyTransactionV1] = {
    internalUpdatePolicyTransactionV1Gen(
      policyIdGen,
      severalAddressGenerator(1, ownersMaxSize),
      Gen.const(ownersToRemove),
      Gen.const(recipientsToAdd),
      Gen.const(recipientsToRemove),
      PermissionsGen.permissionOpTypeGen,
      policyOwner
    )
  }

  def updatePolicyTransactionV2GenWithRecipients(policyOwner: PrivateKeyAccount,
                                                 policyIdGen: Gen[Array[Byte]] = genBoundedString(10, 100),
                                                 recipients: List[PrivateKeyAccount],
                                                 ownersToRemove: List[Address],
                                                 ownersMaxSize: Int = 30,
                                                 feeAssetIdGen: Gen[Option[Array[Byte]]] = Gen.option(bytes32gen)): Gen[UpdatePolicyTransactionV2] = {
    val recipientAddresses = recipients.map(_.toAddress)
    internalUpdatePolicyTransactionV2Gen(
      policyIdGen,
      severalAddressGenerator(1, ownersMaxSize),
      Gen.const(ownersToRemove),
      Gen.const(recipientAddresses),
      PermissionsGen.permissionOpTypeGen,
      feeAssetIdGen,
      policyOwner
    )
  }

  def updatePolicyTransactionV1GenWithOwners(policyOwner: PrivateKeyAccount,
                                             policyIdGen: Gen[Array[Byte]] = genBoundedString(10, 100),
                                             opTypeGen: Gen[OpType] = PermissionsGen.permissionOpTypeGen,
                                             ownersToRemove: List[Address],
                                             ownersMaxSize: Int = 30): Gen[UpdatePolicyTransactionV1] = {
    internalUpdatePolicyTransactionV1Gen(
      policyIdGen,
      severalAddressGenerator(1, ownersMaxSize),
      Gen.const(ownersToRemove),
      Gen.const(List.empty),
      Gen.const(List.empty),
      opTypeGen,
      policyOwner
    )
  }

  def updatePolicyTransactionV2GenWithOwners(policyOwner: PrivateKeyAccount,
                                             policyIdGen: Gen[Array[Byte]] = genBoundedString(10, 100),
                                             opTypeGen: Gen[OpType] = PermissionsGen.permissionOpTypeGen,
                                             ownersToRemove: List[Address],
                                             recipientsMaxSize: Int = 30,
                                             ownersMaxSize: Int = 30,
                                             feeAssetIdGen: Gen[Option[Array[Byte]]] = Gen.option(bytes32gen)): Gen[UpdatePolicyTransactionV2] = {
    internalUpdatePolicyTransactionV2Gen(
      policyIdGen,
      severalAddressGenerator(1, ownersMaxSize),
      Gen.const(ownersToRemove),
      severalAddressGenerator(1, recipientsMaxSize),
      opTypeGen,
      feeAssetIdGen,
      policyOwner
    )
  }

  def createPolicyAndDataHashTransactionsV1Gen(minCount: Int = 1): Gen[(CreatePolicyTransactionV1TestWrap, List[PolicyDataWithTxV1])] = {
    for {
      createPolicy <- createPolicyTransactionV1Gen()
      dataHashTxGen = policyDataHashTransactionV1Gen(Gen.const(createPolicy.tx.id().arr))
      length      <- Gen.choose(minCount, minCount + 10)
      dataHashTxs <- Gen.listOfN(length, dataHashTxGen)
    } yield (createPolicy, dataHashTxs)
  }

  def createPolicyAndDataHashTransactionsV2Gen(minCount: Int = 1): Gen[(CreatePolicyTransactionV2TestWrap, List[PolicyDataWithTxV2])] = {
    for {
      createPolicy <- createPolicyTransactionV2Gen()
      dataHashTxGen = policyDataHashTransactionV2Gen(Gen.const(createPolicy.tx.id().arr))
      length      <- Gen.choose(minCount, minCount + 10)
      dataHashTxs <- Gen.listOfN(length, dataHashTxGen)
    } yield (createPolicy, dataHashTxs)
  }

  val privateDataRequestGen: Gen[PrivateDataRequest] = for {
    policyId <- Gen.alphaNumStr.map(crypto.fastHash).map(ByteStr(_))
    data     <- Gen.alphaNumStr.map(_.getBytes())
    dataHash       = crypto.fastHash(data)
    policyDataHash = PolicyDataHash.fromDataBytes(dataHash)
  } yield PrivateDataRequest(policyId, policyDataHash)

  val privateGotEncryptedDataResponseGen: Gen[GotEncryptedDataResponse] = for {
    policyId <- Gen.alphaNumStr.map(crypto.fastHash).map(ByteStr(_))
    data     <- Gen.alphaNumStr.map(_.getBytes())
    acc1     <- accountGen
    acc2     <- accountGen
    encrypted      = crypto.encrypt(data, acc1.privateKey, acc2.publicKey).explicitGet()
    dataHash       = crypto.fastHash(data)
    policyDataHash = PolicyDataHash.fromDataBytes(dataHash)
  } yield GotEncryptedDataResponse(policyId, policyDataHash, encrypted)

  val privateGotDataResponseGen: Gen[GotDataResponse] = for {
    policyId <- Gen.alphaNumStr.map(crypto.fastHash).map(ByteStr(_))
    data     <- Gen.alphaNumStr.map(crypto.fastHash).map(ByteStr(_))
    dataHash       = crypto.fastHash(data.arr)
    policyDataHash = PolicyDataHash.fromDataBytes(dataHash)
  } yield GotDataResponse(policyId, policyDataHash, data)

  val privateNoDataResponseGen: Gen[NoDataResponse] = for {
    policyId <- Gen.alphaNumStr.map(crypto.fastHash).map(ByteStr(_))
    data     <- Gen.alphaNumStr.map(_.getBytes())
    dataHash       = crypto.fastHash(data)
    policyDataHash = PolicyDataHash.fromDataBytes(dataHash)
  } yield NoDataResponse(policyId, policyDataHash)

  val privateEncryptedDataResponseGen: Gen[PrivateDataResponse] =
    Gen.oneOf(privateGotEncryptedDataResponseGen, privateGotEncryptedDataResponseGen, privateNoDataResponseGen)

  val privateDataResponseGen: Gen[PrivateDataResponse] =
    Gen.oneOf(privateGotDataResponseGen, privateGotDataResponseGen, privateNoDataResponseGen)

  val nodeAttributesGen: Gen[NodeAttributes] = for {
    nodeMode   <- Gen.oneOf(NodeMode.values)
    p2pEnabled <- Gen.oneOf(true, false)
    sender     <- accountGen
  } yield NodeAttributes(nodeMode, p2pEnabled, sender)

  val privacyDataIdGen: Gen[PolicyDataId] = {
    for {
      policyIdBytes <- byteArrayGen(crypto.SignatureLength)
      (_, dataHash) <- policyDataHashGen
    } yield PolicyDataId(ByteStr(policyIdBytes), dataHash)
  }

  val policyMetaData: Gen[PolicyMetaData] = {
    for {
      policyId  <- Gen.asciiPrintableStr
      hash      <- Gen.asciiPrintableStr
      sender    <- Gen.asciiPrintableStr
      filename  <- Gen.asciiPrintableStr
      size      <- Gen.posNum[Int]
      timestamp <- Gen.posNum[Long]
      author    <- Gen.asciiPrintableStr
      comment   <- Gen.asciiPrintableStr
    } yield PolicyMetaData(policyId, hash, sender, filename, size, timestamp, author, comment, None)
  }

  def issueAndSendSponsorAssetsGen(master: PrivateKeyAccount,
                                   createFeeInAsset: Long,
                                   startTime: Long): Gen[(Address, AssetId, GenesisTransaction, Block, Long)] =
    for {
      sponsorIssuer <- accountGen
      sponsorIssue = IssueTransactionV2
        .selfSigned(currentChainId,
                    sponsorIssuer,
                    SponsorAssetName,
                    SponsorAssetName,
                    ENOUGH_AMT,
                    2,
                    reissuable = false,
                    IssueFee,
                    startTime + 1.minute.toMillis,
                    None)
        .explicitGet()
      sponsorAssetId = sponsorIssue.id()
      sponsorActivation = SponsorFeeTransactionV1
        .selfSigned(sponsorIssuer, sponsorAssetId, isEnabled = true, SponsorshipFee, startTime + 2.minute.toMillis)
        .explicitGet()
      assetTransfer = TransferTransactionV2
        .selfSigned(sponsorIssuer,
                    Some(sponsorAssetId),
                    None,
                    startTime + 3.minutes.toMillis,
                    100 * createFeeInAsset,
                    TransferFee,
                    master.toAddress,
                    Array.empty)
        .explicitGet()
      sponsorGenesis = GenesisTransaction.create(sponsorIssuer.toAddress, ENOUGH_AMT, startTime).explicitGet()
      sponsorBlock   = TestBlock.create(Seq(sponsorIssue, sponsorActivation, assetTransfer))
    } yield (sponsorIssuer.toAddress, sponsorAssetId, sponsorGenesis, sponsorBlock, startTime + 3.minutes.toMillis)

  def accountGenesisGen(startTime: Long): Gen[(PrivateKeyAccount, GenesisTransaction)] =
    for {
      account <- accountGen
      genesis = GenesisTransaction.create(account.toAddress, ENOUGH_AMT, startTime).explicitGet()
    } yield (account, genesis)
}

object TransactionGen {
  val SponsorAssetName = "SponsorAsset".getBytes(StandardCharsets.UTF_8)
  val IssueFee         = TestFees.defaultFees.forTxType(IssueTransactionV2.typeId)
  val SponsorshipFee   = TestFees.defaultFees.forTxType(SponsorFeeTransactionV1.typeId)
  val TransferFee      = TestFees.defaultFees.forTxType(TransferTransaction.typeId)
  val SetScriptFee     = TestFees.defaultFees.forTxType(SetAssetScriptTransaction.typeId)
}
