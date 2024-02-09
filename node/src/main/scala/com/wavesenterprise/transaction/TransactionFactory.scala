package com.wavesenterprise.transaction

import cats.implicits._
import com.google.common.base.Charsets
import com.wavesenterprise.account._
import com.wavesenterprise.acl.OpType
import com.wavesenterprise.api.http.DataRequestV1._
import com.wavesenterprise.api.http.DataRequestV2._
import com.wavesenterprise.api.http.DataRequestV3._
import com.wavesenterprise.api.http.acl.{PermitRequestV1, PermitRequestV2, SignedPermitRequestV1, SignedPermitRequestV2}
import com.wavesenterprise.api.http.alias.{
  CreateAliasV2Request,
  CreateAliasV3Request,
  CreateAliasV4Request,
  SignedCreateAliasV2Request,
  SignedCreateAliasV3Request,
  SignedCreateAliasV4Request
}
import com.wavesenterprise.api.http.assets.SponsorFeeRequest._
import com.wavesenterprise.api.http.assets._
import com.wavesenterprise.api.http.docker._
import com.wavesenterprise.api.http.leasing._
import com.wavesenterprise.api.http.service.PrivacyApiService
import com.wavesenterprise.api.http._
import com.wavesenterprise.api.http.privacy.{
  CreatePolicyRequestV1,
  CreatePolicyRequestV2,
  CreatePolicyRequestV3,
  PolicyDataHashRequestV3,
  SignedCreatePolicyRequestV1,
  SignedCreatePolicyRequestV2,
  SignedCreatePolicyRequestV3,
  SignedPolicyDataHashRequestV3,
  SignedUpdatePolicyRequestV1,
  SignedUpdatePolicyRequestV2,
  SignedUpdatePolicyRequestV3,
  UpdatePolicyRequestV1,
  UpdatePolicyRequestV2,
  UpdatePolicyRequestV3
}
import com.wavesenterprise.crypto.internals.confidentialcontracts.Commitment
import com.wavesenterprise.api.http.privacy.{
  CreatePolicyRequestV1,
  CreatePolicyRequestV2,
  CreatePolicyRequestV3,
  PolicyDataHashRequestV3,
  SignedCreatePolicyRequestV1,
  SignedCreatePolicyRequestV2,
  SignedCreatePolicyRequestV3,
  SignedPolicyDataHashRequestV3,
  SignedUpdatePolicyRequestV1,
  SignedUpdatePolicyRequestV2,
  SignedUpdatePolicyRequestV3,
  UpdatePolicyRequestV1,
  UpdatePolicyRequestV2,
  UpdatePolicyRequestV3
}
import com.wavesenterprise.api.http.wasm.{
  CallContractRequestV7,
  CreateContractRequestV7,
  SignedCreateContractRequestV7,
  SignedUpdateContractRequestV6,
  UpdateContractRequestV6
}
import com.wavesenterprise.privacy.PolicyDataHash
import com.wavesenterprise.serialization.TxAdapter
import com.wavesenterprise.state.ByteStr
import com.wavesenterprise.transaction.ValidationError.{EmptySenderPKInDataTxError, GenericError, InvalidContractId}
import com.wavesenterprise.transaction.acl.{PermitTransactionV1, PermitTransactionV2}
import com.wavesenterprise.transaction.assets._
import com.wavesenterprise.transaction.assets.exchange.ExchangeTransactionV2
import com.wavesenterprise.transaction.docker._
import com.wavesenterprise.transaction.lease.{LeaseCancelTransactionV2, LeaseCancelTransactionV3, LeaseTransactionV2, LeaseTransactionV3}
import com.wavesenterprise.transaction.protobuf.{Transaction => PbTransaction}
import com.wavesenterprise.transaction.smart.script.Script
import com.wavesenterprise.transaction.smart.{SetScriptTransaction, SetScriptTransactionV1}
import com.wavesenterprise.transaction.transfer._
import com.wavesenterprise.transaction.validation.ExecutableValidation.getContractApiVersion
import com.wavesenterprise.transaction.validation.PolicyValidation
import com.wavesenterprise.utils.{Base58, Base64, ScorexLogging, Time}
import com.wavesenterprise.wallet.Wallet
import play.api.libs.json.JsValue

object TransactionFactory extends ScorexLogging {

  private def findPrivateKey(wallet: Wallet, request: UnsignedTxRequest): Either[ValidationError, PrivateKeyAccount] = {

    /**
      * Data Transactions are signed by author, not sender
      */
    val signer = request match {
      case dataReq: DataRequest => dataReq.author
      case _                    => request.sender
    }
    log.trace(s"looking for privateKey for '$signer'")
    wallet.findPrivateKey(signer, request.password.map(_.toCharArray))
  }

  def transferAssetV2(request: TransferV2Request, wallet: Wallet, time: Time): Either[ValidationError, TransferTransactionV2] = {
    for {
      sender <- findPrivateKey(wallet, request)
      _ = log.trace(s"found privateKey for address: '${sender.address}', publicKey: '${sender.publicKeyBase58}'")
      recipientAcc <- AddressOrAlias.fromString(request.recipient).leftMap(ValidationError.fromCryptoError)
      _ = log.trace(s"found recipientAcc '${recipientAcc.stringRepr}' for address: '${sender.address}', publicKey: '${sender.publicKeyBase58}'")
      attachment <- decodeTransferAttachment(request.attachment)
      feeAssetId <- request.decodeFeeAssetId()
      tx <- TransferTransactionV2.selfSigned(
        sender,
        request.assetId.map(s => ByteStr.decodeBase58(s).get),
        feeAssetId,
        request.timestamp.getOrElse(time.getTimestamp()),
        request.amount,
        request.fee,
        recipientAcc,
        attachment
      )
    } yield tx
  }

  def transferAssetV2(request: TransferV2Request, sender: PublicKeyAccount): Either[ValidationError, TransferTransactionV2] =
    for {
      recipientAcc <- AddressOrAlias.fromString(request.recipient).leftMap(ValidationError.fromCryptoError)
      attachment   <- decodeTransferAttachment(request.attachment)
      feeAssetId   <- request.decodeFeeAssetId()
      tx <- TransferTransactionV2.create(sender,
                                         request.assetId.map(s => ByteStr.decodeBase58(s).get),
                                         feeAssetId,
                                         0,
                                         request.amount,
                                         request.fee,
                                         recipientAcc,
                                         attachment,
                                         Proofs.empty)
    } yield tx

  def transferAssetV3(request: TransferV3Request, wallet: Wallet, time: Time): Either[ValidationError, TransferTransactionV3] = {
    for {
      sender <- findPrivateKey(wallet, request)
      _ = log.trace(s"found privateKey for address: '${sender.address}', publicKey: '${sender.publicKeyBase58}'")
      recipientAcc <- AddressOrAlias.fromString(request.recipient).leftMap(ValidationError.fromCryptoError)
      _ = log.trace(s"found recipientAcc '${recipientAcc.stringRepr}' for address: '${sender.address}', publicKey: '${sender.publicKeyBase58}'")
      attachment <- decodeTransferAttachment(request.attachment)
      feeAssetId <- request.decodeFeeAssetId()
      tx <- TransferTransactionV3.selfSigned(
        sender,
        request.assetId.map(s => ByteStr.decodeBase58(s).get),
        feeAssetId,
        request.timestamp.getOrElse(time.getTimestamp()),
        request.amount,
        request.fee,
        recipientAcc,
        attachment,
        request.atomicBadge
      )
    } yield tx
  }

  def transferAssetV3(request: TransferV3Request, sender: PublicKeyAccount): Either[ValidationError, TransferTransactionV3] =
    for {
      recipientAcc <- AddressOrAlias.fromString(request.recipient).leftMap(ValidationError.fromCryptoError)
      attachment   <- decodeTransferAttachment(request.attachment)
      feeAssetId   <- request.decodeFeeAssetId()
      tx <- TransferTransactionV3.create(
        sender,
        request.assetId.map(s => ByteStr.decodeBase58(s).get),
        feeAssetId,
        0,
        request.amount,
        request.fee,
        recipientAcc,
        attachment,
        request.atomicBadge,
        Proofs.empty
      )
    } yield tx

  def massTransferAssetV1(request: MassTransferRequestV1, wallet: Wallet, time: Time): Either[ValidationError, MassTransferTransactionV1] =
    for {
      sender     <- findPrivateKey(wallet, request)
      transfers  <- MassTransferRequest.parseTransfersList(request.transfers)
      attachment <- decodeTransferAttachment(request.attachment)
      tx <- MassTransferTransactionV1.selfSigned(
        sender,
        request.assetId.map(s => ByteStr.decodeBase58(s).get),
        transfers,
        request.timestamp.getOrElse(time.getTimestamp()),
        request.fee,
        attachment
      )
    } yield tx

  def massTransferAssetV1(request: MassTransferRequestV1, sender: PublicKeyAccount): Either[ValidationError, MassTransferTransactionV1] =
    for {
      transfers  <- MassTransferRequest.parseTransfersList(request.transfers)
      attachment <- decodeTransferAttachment(request.attachment)
      tx <- MassTransferTransactionV1.create(
        sender,
        request.assetId.map(s => ByteStr.decodeBase58(s).get),
        transfers,
        0,
        request.fee,
        attachment,
        Proofs.empty
      )
    } yield tx

  def massTransferAssetV2(request: MassTransferRequestV2, wallet: Wallet, time: Time): Either[ValidationError, MassTransferTransactionV2] =
    for {
      sender     <- findPrivateKey(wallet, request)
      transfers  <- MassTransferRequest.parseTransfersList(request.transfers)
      attachment <- decodeTransferAttachment(request.attachment)
      tx <- MassTransferTransactionV2.selfSigned(
        sender,
        request.assetId.map(s => ByteStr.decodeBase58(s).get),
        transfers,
        request.timestamp.getOrElse(time.getTimestamp()),
        request.fee,
        attachment,
        request.feeAssetId.map(s => ByteStr.decodeBase58(s).get)
      )
    } yield tx

  def massTransferAssetV2(request: MassTransferRequestV2, sender: PublicKeyAccount): Either[ValidationError, MassTransferTransactionV2] =
    for {
      transfers  <- MassTransferRequest.parseTransfersList(request.transfers)
      attachment <- decodeTransferAttachment(request.attachment)
      tx <- MassTransferTransactionV2.create(
        sender,
        request.assetId.map(s => ByteStr.decodeBase58(s).get),
        transfers,
        0,
        request.fee,
        attachment,
        request.feeAssetId.map(s => ByteStr.decodeBase58(s).get),
        Proofs.empty
      )
    } yield tx

  def massTransferAssetV3(request: MassTransferRequestV3, wallet: Wallet, time: Time): Either[ValidationError, MassTransferTransactionV3] =
    for {
      sender     <- findPrivateKey(wallet, request)
      transfers  <- MassTransferRequest.parseTransfersList(request.transfers)
      attachment <- decodeTransferAttachment(request.attachment)
      tx <- MassTransferTransactionV3.selfSigned(
        sender,
        request.assetId.map(s => ByteStr.decodeBase58(s).get),
        transfers,
        request.timestamp.getOrElse(time.getTimestamp()),
        request.fee,
        attachment,
        request.feeAssetId.map(s => ByteStr.decodeBase58(s).get),
        atomicBadge = request.atomicBadge
      )
    } yield tx

  def massTransferAssetV3(request: MassTransferRequestV3, sender: PublicKeyAccount): Either[ValidationError, MassTransferTransactionV3] =
    for {
      transfers  <- MassTransferRequest.parseTransfersList(request.transfers)
      attachment <- decodeTransferAttachment(request.attachment)
      tx <- MassTransferTransactionV3.create(
        sender,
        request.assetId.map(s => ByteStr.decodeBase58(s).get),
        transfers,
        0,
        request.fee,
        attachment,
        request.feeAssetId.map(s => ByteStr.decodeBase58(s).get),
        atomicBadge = request.atomicBadge,
        Proofs.empty
      )
    } yield tx

  def setScript(request: SetScriptRequest, wallet: Wallet, time: Time): Either[ValidationError, SetScriptTransaction] =
    for {
      sender <- findPrivateKey(wallet, request)
      script <- request.script match {
        case Some(s) if s.nonEmpty => Script.fromBase64String(s).map(Some(_))
        case _                     => Right(None)
      }
      tx <- SetScriptTransactionV1.selfSigned(
        AddressScheme.getAddressSchema.chainId,
        sender,
        script,
        request.name.getBytes(Charsets.UTF_8),
        request.description.getOrElse("").getBytes(Charsets.UTF_8),
        request.fee,
        request.timestamp.getOrElse(time.getTimestamp())
      )
    } yield tx

  def setScript(request: SetScriptRequest, sender: PublicKeyAccount): Either[ValidationError, SetScriptTransaction] =
    for {
      script <- request.script match {
        case Some(s) if s.nonEmpty => Script.fromBase64String(s).map(Some(_))
        case _                     => Right(None)
      }
      tx <- SetScriptTransactionV1.create(
        AddressScheme.getAddressSchema.chainId,
        sender,
        script,
        request.name.getBytes(Charsets.UTF_8),
        request.description.getOrElse("").getBytes(Charsets.UTF_8),
        request.fee,
        0,
        Proofs.empty
      )
    } yield tx

  def setAssetScript(request: SetAssetScriptRequest, wallet: Wallet, time: Time): Either[ValidationError, SetAssetScriptTransactionV1] =
    for {
      sender <- findPrivateKey(wallet, request)
      script <- request.script match {
        case None | Some("") => Right(None)
        case Some(s)         => Script.fromBase64String(s).map(Some(_))
      }
      tx <- SetAssetScriptTransactionV1.selfSigned(
        AddressScheme.getAddressSchema.chainId,
        sender,
        ByteStr.decodeBase58(request.assetId).get,
        script,
        request.fee,
        request.timestamp.getOrElse(time.getTimestamp())
      )
    } yield tx

  def setAssetScript(request: SetAssetScriptRequest, sender: PublicKeyAccount): Either[ValidationError, SetAssetScriptTransactionV1] =
    for {
      script <- request.script match {
        case Some(s) if s.nonEmpty => Script.fromBase64String(s).map(Some(_))
        case _                     => Right(None)
      }
      tx <- SetAssetScriptTransactionV1.create(
        AddressScheme.getAddressSchema.chainId,
        sender,
        ByteStr.decodeBase58(request.assetId).get,
        script,
        request.fee,
        request.timestamp.getOrElse(0),
        Proofs.empty
      )
    } yield tx

  def issueAssetV2(request: IssueV2Request, wallet: Wallet, time: Time): Either[ValidationError, IssueTransactionV2] =
    for {
      sender <- findPrivateKey(wallet, request)
      s <- request.script match {
        case None    => Right(None)
        case Some(s) => Script.fromBase64String(s).map(Some(_))
      }
      tx <- IssueTransactionV2.selfSigned(
        chainId = AddressScheme.getAddressSchema.chainId,
        sender = sender,
        name = request.name.getBytes(Charsets.UTF_8),
        description = request.description.getBytes(Charsets.UTF_8),
        quantity = request.quantity,
        decimals = request.decimals,
        reissuable = request.reissuable,
        fee = request.fee,
        timestamp = request.timestamp.getOrElse(time.getTimestamp()),
        script = s
      )
    } yield tx

  def issueAssetV2(request: IssueV2Request, sender: PublicKeyAccount): Either[ValidationError, IssueTransactionV2] =
    for {
      s <- request.script match {
        case None    => Right(None)
        case Some(s) => Script.fromBase64String(s).map(Some(_))
      }
      tx <- IssueTransactionV2.create(
        chainId = AddressScheme.getAddressSchema.chainId,
        sender = sender,
        name = request.name.getBytes(Charsets.UTF_8),
        description = request.description.getBytes(Charsets.UTF_8),
        quantity = request.quantity,
        decimals = request.decimals,
        reissuable = request.reissuable,
        script = s,
        fee = request.fee,
        timestamp = 0,
        proofs = Proofs.empty
      )
    } yield tx

  def issueAssetV3(request: IssueV3Request, wallet: Wallet, time: Time): Either[ValidationError, IssueTransactionV3] =
    for {
      sender <- findPrivateKey(wallet, request)
      s <- request.script match {
        case None    => Right(None)
        case Some(s) => Script.fromBase64String(s).map(Some(_))
      }
      tx <- IssueTransactionV3.selfSigned(
        chainId = AddressScheme.getAddressSchema.chainId,
        sender = sender,
        name = request.name.getBytes(Charsets.UTF_8),
        description = request.description.getBytes(Charsets.UTF_8),
        quantity = request.quantity,
        decimals = request.decimals,
        reissuable = request.reissuable,
        fee = request.fee,
        timestamp = request.timestamp.getOrElse(time.getTimestamp()),
        script = s,
        atomicBadge = request.atomicBadge
      )
    } yield tx

  def issueAssetV3(request: IssueV3Request, sender: PublicKeyAccount): Either[ValidationError, IssueTransactionV3] =
    for {
      s <- request.script match {
        case None    => Right(None)
        case Some(s) => Script.fromBase64String(s).map(Some(_))
      }
      tx <- IssueTransactionV3.create(
        chainId = AddressScheme.getAddressSchema.chainId,
        sender = sender,
        name = request.name.getBytes(Charsets.UTF_8),
        description = request.description.getBytes(Charsets.UTF_8),
        quantity = request.quantity,
        decimals = request.decimals,
        reissuable = request.reissuable,
        script = s,
        fee = request.fee,
        timestamp = 0,
        atomicBadge = request.atomicBadge,
        proofs = Proofs.empty
      )
    } yield tx

  def leaseV2(request: LeaseV2Request, wallet: Wallet, time: Time): Either[ValidationError, LeaseTransactionV2] =
    for {
      sender       <- findPrivateKey(wallet, request)
      recipientAcc <- AddressOrAlias.fromString(request.recipient).leftMap(ValidationError.fromCryptoError)
      tx           <- LeaseTransactionV2.selfSigned(None, sender, recipientAcc, request.amount, request.fee, request.timestamp.getOrElse(time.getTimestamp()))
    } yield tx

  def leaseV2(request: LeaseV2Request, sender: PublicKeyAccount): Either[ValidationError, LeaseTransactionV2] =
    for {
      recipientAcc <- AddressOrAlias.fromString(request.recipient).leftMap(ValidationError.fromCryptoError)
      tx           <- LeaseTransactionV2.create(None, sender, recipientAcc, request.amount, request.fee, 0, Proofs.empty)
    } yield tx

  def leaseV3(request: LeaseV3Request, wallet: Wallet, time: Time): Either[ValidationError, LeaseTransactionV3] =
    for {
      sender       <- findPrivateKey(wallet, request)
      recipientAcc <- AddressOrAlias.fromString(request.recipient).leftMap(ValidationError.fromCryptoError)
      tx <- LeaseTransactionV3.selfSigned(None,
                                          sender,
                                          recipientAcc,
                                          request.amount,
                                          request.fee,
                                          request.timestamp.getOrElse(time.getTimestamp()),
                                          request.atomicBadge)
    } yield tx

  def leaseV3(request: LeaseV3Request, sender: PublicKeyAccount): Either[ValidationError, LeaseTransactionV3] =
    for {
      recipientAcc <- AddressOrAlias.fromString(request.recipient).leftMap(ValidationError.fromCryptoError)
      tx           <- LeaseTransactionV3.create(None, sender, recipientAcc, request.amount, request.fee, 0, request.atomicBadge, Proofs.empty)
    } yield tx

  def leaseCancelV2(request: LeaseCancelV2Request, wallet: Wallet, time: Time): Either[ValidationError, LeaseCancelTransactionV2] =
    for {
      sender <- findPrivateKey(wallet, request)
      tx <- LeaseCancelTransactionV2.selfSigned(AddressScheme.getAddressSchema.chainId,
                                                sender,
                                                request.fee,
                                                request.timestamp.getOrElse(time.getTimestamp()),
                                                ByteStr.decodeBase58(request.txId).get)
    } yield tx

  def leaseCancelV2(request: LeaseCancelV2Request, sender: PublicKeyAccount): Either[ValidationError, LeaseCancelTransactionV2] =
    LeaseCancelTransactionV2.create(AddressScheme.getAddressSchema.chainId,
                                    sender,
                                    request.fee,
                                    0,
                                    ByteStr.decodeBase58(request.txId).get,
                                    Proofs.empty)

  def leaseCancelV3(request: LeaseCancelV3Request, wallet: Wallet, time: Time): Either[ValidationError, LeaseCancelTransactionV3] =
    for {
      sender <- findPrivateKey(wallet, request)
      tx <- LeaseCancelTransactionV3.selfSigned(
        AddressScheme.getAddressSchema.chainId,
        sender,
        request.fee,
        request.timestamp.getOrElse(time.getTimestamp()),
        ByteStr.decodeBase58(request.txId).get,
        request.atomicBadge
      )
    } yield tx

  def leaseCancelV3(request: LeaseCancelV3Request, sender: PublicKeyAccount): Either[ValidationError, LeaseCancelTransactionV3] =
    LeaseCancelTransactionV3.create(AddressScheme.getAddressSchema.chainId,
                                    sender,
                                    request.fee,
                                    0,
                                    ByteStr.decodeBase58(request.txId).get,
                                    request.atomicBadge,
                                    Proofs.empty)

  def aliasV2(request: CreateAliasV2Request, wallet: Wallet, time: Time): Either[ValidationError, CreateAliasTransactionV2] =
    for {
      sender <- findPrivateKey(wallet, request)
      alias  <- Alias.buildWithCurrentChainId(request.alias).leftMap(ValidationError.fromCryptoError)
      tx <- CreateAliasTransactionV2.selfSigned(
        sender,
        alias,
        request.fee,
        timestamp = request.timestamp.getOrElse(time.getTimestamp())
      )
    } yield tx

  def aliasV2(request: CreateAliasV2Request, sender: PublicKeyAccount): Either[ValidationError, CreateAliasTransactionV2] =
    for {
      alias <- Alias.buildWithCurrentChainId(request.alias).leftMap(ValidationError.fromCryptoError)
      tx <- CreateAliasTransactionV2.create(
        sender,
        alias,
        request.fee,
        0,
        Proofs.empty
      )
    } yield tx

  def aliasV3(request: CreateAliasV3Request, wallet: Wallet, time: Time): Either[ValidationError, CreateAliasTransactionV3] =
    for {
      sender <- findPrivateKey(wallet, request)
      alias  <- Alias.buildWithCurrentChainId(request.alias).leftMap(ValidationError.fromCryptoError)
      tx <- CreateAliasTransactionV3.selfSigned(
        sender,
        alias,
        request.fee,
        request.timestamp.getOrElse(time.getTimestamp()),
        request.feeAssetId.map(s => ByteStr.decodeBase58(s).get)
      )
    } yield tx

  def aliasV3(request: CreateAliasV3Request, sender: PublicKeyAccount): Either[ValidationError, CreateAliasTransactionV3] =
    for {
      alias <- Alias.buildWithCurrentChainId(request.alias).leftMap(ValidationError.fromCryptoError)
      tx <- CreateAliasTransactionV3.create(
        sender,
        alias,
        request.fee,
        0,
        request.feeAssetId.map(s => ByteStr.decodeBase58(s).get),
        Proofs.empty
      )
    } yield tx

  def aliasV4(request: CreateAliasV4Request, wallet: Wallet, time: Time): Either[ValidationError, CreateAliasTransactionV4] =
    for {
      sender <- findPrivateKey(wallet, request)
      alias  <- Alias.buildWithCurrentChainId(request.alias).leftMap(ValidationError.fromCryptoError)
      tx <- CreateAliasTransactionV4.selfSigned(
        sender,
        alias,
        request.fee,
        request.timestamp.getOrElse(time.getTimestamp()),
        request.feeAssetId.map(s => ByteStr.decodeBase58(s).get),
        request.atomicBadge
      )
    } yield tx

  def aliasV4(request: CreateAliasV4Request, sender: PublicKeyAccount): Either[ValidationError, CreateAliasTransactionV4] =
    for {
      alias <- Alias.buildWithCurrentChainId(request.alias).leftMap(ValidationError.fromCryptoError)
      tx <- CreateAliasTransactionV4.create(
        sender,
        alias,
        request.fee,
        0,
        request.feeAssetId.map(s => ByteStr.decodeBase58(s).get),
        request.atomicBadge,
        Proofs.empty
      )
    } yield tx

  def reissueAssetV2(request: ReissueV2Request, wallet: Wallet, time: Time): Either[ValidationError, ReissueTransactionV2] =
    for {
      sender <- findPrivateKey(wallet, request)
      tx <- ReissueTransactionV2.selfSigned(
        AddressScheme.getAddressSchema.chainId,
        sender,
        ByteStr.decodeBase58(request.assetId).get,
        request.quantity,
        request.reissuable,
        request.fee,
        request.timestamp.getOrElse(time.getTimestamp())
      )
    } yield tx

  def reissueAssetV2(request: ReissueV2Request, sender: PublicKeyAccount): Either[ValidationError, ReissueTransactionV2] =
    ReissueTransactionV2.create(
      AddressScheme.getAddressSchema.chainId,
      sender,
      ByteStr.decodeBase58(request.assetId).get,
      request.quantity,
      request.reissuable,
      request.fee,
      0,
      Proofs.empty
    )

  def reissueAssetV3(request: ReissueV3Request, wallet: Wallet, time: Time): Either[ValidationError, ReissueTransactionV3] =
    for {
      sender <- findPrivateKey(wallet, request)
      tx <- ReissueTransactionV3.selfSigned(
        AddressScheme.getAddressSchema.chainId,
        sender,
        ByteStr.decodeBase58(request.assetId).get,
        request.quantity,
        request.reissuable,
        request.fee,
        request.timestamp.getOrElse(time.getTimestamp()),
        request.atomicBadge
      )
    } yield tx

  def reissueAssetV3(request: ReissueV3Request, sender: PublicKeyAccount): Either[ValidationError, ReissueTransactionV3] =
    ReissueTransactionV3.create(
      AddressScheme.getAddressSchema.chainId,
      sender,
      ByteStr.decodeBase58(request.assetId).get,
      request.quantity,
      request.reissuable,
      request.fee,
      0,
      request.atomicBadge,
      Proofs.empty
    )

  def burnAssetV2(request: BurnV2Request, wallet: Wallet, time: Time): Either[ValidationError, BurnTransactionV2] =
    for {
      sender <- findPrivateKey(wallet, request)
      tx <- BurnTransactionV2.selfSigned(
        AddressScheme.getAddressSchema.chainId,
        sender,
        ByteStr.decodeBase58(request.assetId).get,
        request.quantity,
        request.fee,
        request.timestamp.getOrElse(time.getTimestamp())
      )
    } yield tx

  def burnAssetV2(request: BurnV2Request, sender: PublicKeyAccount): Either[ValidationError, BurnTransactionV2] = BurnTransactionV2.create(
    AddressScheme.getAddressSchema.chainId,
    sender,
    ByteStr.decodeBase58(request.assetId).get,
    request.quantity,
    request.fee,
    0,
    Proofs.empty
  )

  def burnAssetV3(request: BurnV3Request, wallet: Wallet, time: Time): Either[ValidationError, BurnTransactionV3] =
    for {
      sender <- findPrivateKey(wallet, request)
      tx <- BurnTransactionV3.selfSigned(
        AddressScheme.getAddressSchema.chainId,
        sender,
        ByteStr.decodeBase58(request.assetId).get,
        request.quantity,
        request.fee,
        request.timestamp.getOrElse(time.getTimestamp()),
        request.atomicBadge
      )
    } yield tx

  def burnAssetV3(request: BurnV3Request, sender: PublicKeyAccount): Either[ValidationError, BurnTransactionV3] = BurnTransactionV3.create(
    AddressScheme.getAddressSchema.chainId,
    sender,
    ByteStr.decodeBase58(request.assetId).get,
    request.quantity,
    request.fee,
    0,
    request.atomicBadge,
    Proofs.empty
  )

  def dataV1(request: DataRequestV1, wallet: Wallet, time: Time): Either[ValidationError, DataTransactionV1] =
    for {
      sa <- dataSenderAuthor(request, wallet)
      (sender, author) = sa
      tx <- DataTransactionV1.signed(author, sender, author, request.data, request.timestamp.getOrElse(time.getTimestamp()), request.fee)
    } yield tx

  def dataV2(request: DataRequestV2, wallet: Wallet, time: Time): Either[ValidationError, DataTransactionV2] =
    for {
      sa <- dataSenderAuthor(request, wallet)
      (sender, author) = sa
      tx <- DataTransactionV2.signed(author,
                                     sender,
                                     author,
                                     request.data,
                                     request.timestamp.getOrElse(time.getTimestamp()),
                                     request.fee,
                                     request.feeAssetId.map(s => ByteStr.decodeBase58(s).get))
    } yield tx

  def dataV3(request: DataRequestV3, wallet: Wallet, time: Time): Either[ValidationError, DataTransactionV3] =
    for {
      sa <- dataSenderAuthor(request, wallet)
      (sender, author) = sa
      tx <- DataTransactionV3.signed(
        author,
        sender,
        author,
        request.data,
        request.timestamp.getOrElse(time.getTimestamp()),
        request.fee,
        request.feeAssetId.map(s => ByteStr.decodeBase58(s).get),
        request.atomicBadge
      )
    } yield tx

  private def dataSenderAuthor(request: DataRequest, wallet: Wallet) = {
    def extractPK(author: PrivateKeyAccount): Either[ValidationError, String] = {
      if (request.author == request.sender) Right(author.publicKeyBase58)
      else Either.cond(request.senderPublicKey.isDefined, request.senderPublicKey.get, EmptySenderPKInDataTxError)
    }

    for {
      author   <- findPrivateKey(wallet, request)
      senderPk <- extractPK(author)
      sender   <- PublicKeyAccount.fromBase58String(senderPk).leftMap(ValidationError.fromCryptoError)
    } yield (sender, author)
  }

  def dataV1(request: DataRequestV1, sender: PublicKeyAccount, author: PublicKeyAccount): Either[ValidationError, DataTransactionV1] =
    DataTransactionV1.create(sender, author, request.data, 0, request.fee, Proofs.empty)

  def dataV2(request: DataRequestV2, sender: PublicKeyAccount, author: PublicKeyAccount): Either[ValidationError, DataTransactionV2] =
    DataTransactionV2.create(sender, author, request.data, 0, request.fee, request.feeAssetId.map(s => ByteStr.decodeBase58(s).get), Proofs.empty)

  def dataV3(request: DataRequestV3, sender: PublicKeyAccount, author: PublicKeyAccount): Either[ValidationError, DataTransactionV3] =
    DataTransactionV3.create(sender,
                             author,
                             request.data,
                             0,
                             request.fee,
                             request.feeAssetId.map(s => ByteStr.decodeBase58(s).get),
                             request.atomicBadge,
                             Proofs.empty)

  def sponsor(request: SponsorFeeRequest, wallet: Wallet, time: Time): Either[ValidationError, SponsorFeeTransactionV1] =
    for {
      sender  <- findPrivateKey(wallet, request)
      assetId <- ByteStr.decodeBase58(request.assetId).toEither.left.map(_ => GenericError(s"Wrong Base58 string: ${request.assetId}"))
      tx <- SponsorFeeTransactionV1.selfSigned(
        sender,
        assetId,
        request.isEnabled,
        request.fee,
        request.timestamp.getOrElse(time.getTimestamp())
      )
    } yield tx

  def sponsor(request: SponsorFeeRequest, sender: PublicKeyAccount): Either[ValidationError, SponsorFeeTransactionV1] =
    for {
      assetId <- ByteStr.decodeBase58(request.assetId).toEither.left.map(_ => GenericError(s"Wrong Base58 string: ${request.assetId}"))
      tx <- SponsorFeeTransactionV1.create(
        sender,
        assetId,
        request.isEnabled,
        request.fee,
        0,
        Proofs.empty
      )
    } yield tx

  def sponsorV2(request: SponsorFeeRequestV2, wallet: Wallet, time: Time): Either[ValidationError, SponsorFeeTransactionV2] =
    for {
      sender  <- findPrivateKey(wallet, request)
      assetId <- ByteStr.decodeBase58(request.assetId).toEither.left.map(_ => GenericError(s"Wrong Base58 string: ${request.assetId}"))
      tx <- SponsorFeeTransactionV2.selfSigned(
        sender,
        assetId,
        request.isEnabled,
        request.fee,
        request.timestamp.getOrElse(time.getTimestamp()),
        request.atomicBadge
      )
    } yield tx

  def sponsorV2(request: SponsorFeeRequestV2, sender: PublicKeyAccount): Either[ValidationError, SponsorFeeTransactionV2] =
    for {
      assetId <- ByteStr.decodeBase58(request.assetId).toEither.left.map(_ => GenericError(s"Wrong Base58 string: ${request.assetId}"))
      tx <- SponsorFeeTransactionV2.create(
        sender,
        assetId,
        request.isEnabled,
        request.fee,
        0,
        request.atomicBadge,
        Proofs.empty
      )
    } yield tx

  def fromSignedRequest(jsv: JsValue): Either[ValidationError, Transaction] = {
    val typeId  = (jsv \ "type").as[Byte]
    val version = (jsv \ "version").asOpt[Byte](versionReads).getOrElse(1.toByte)
    TransactionParsers.by(typeId, version) match {
      case None =>
        Left(GenericError(s"Bad transaction type ($typeId) and version ($version)"))

      case Some(x) =>
        x match {
          case IssueTransactionV2          => jsv.as[SignedIssueV2Request].toTx
          case IssueTransactionV3          => jsv.as[SignedIssueV3Request].toTx
          case TransferTransactionV2       => jsv.as[SignedTransferV2Request].toTx
          case TransferTransactionV3       => jsv.as[SignedTransferV3Request].toTx
          case MassTransferTransactionV1   => jsv.as[SignedMassTransferRequestV1].toTx
          case MassTransferTransactionV2   => jsv.as[SignedMassTransferRequestV2].toTx
          case MassTransferTransactionV3   => jsv.as[SignedMassTransferRequestV3].toTx
          case ReissueTransactionV2        => jsv.as[SignedReissueV2Request].toTx
          case ReissueTransactionV3        => jsv.as[SignedReissueV3Request].toTx
          case BurnTransactionV2           => jsv.as[SignedBurnV2Request].toTx
          case BurnTransactionV3           => jsv.as[SignedBurnV3Request].toTx
          case LeaseTransactionV2          => jsv.as[SignedLeaseV2Request].toTx
          case LeaseTransactionV3          => jsv.as[SignedLeaseV3Request].toTx
          case LeaseCancelTransactionV2    => jsv.as[SignedLeaseCancelV2Request].toTx
          case LeaseCancelTransactionV3    => jsv.as[SignedLeaseCancelV3Request].toTx
          case CreateAliasTransactionV2    => jsv.as[SignedCreateAliasV2Request].toTx
          case CreateAliasTransactionV3    => jsv.as[SignedCreateAliasV3Request].toTx
          case CreateAliasTransactionV4    => jsv.as[SignedCreateAliasV4Request].toTx
          case DataTransactionV1           => jsv.as[SignedDataRequestV1].toTx
          case DataTransactionV2           => jsv.as[SignedDataRequestV2].toTx
          case DataTransactionV3           => jsv.as[SignedDataRequestV3].toTx
          case SetScriptTransactionV1      => jsv.as[SignedSetScriptRequest].toTx
          case SetAssetScriptTransactionV1 => jsv.as[SignedSetAssetScriptRequest].toTx
          case SponsorFeeTransactionV1     => jsv.as[SignedSponsorFeeRequest].toTx
          case SponsorFeeTransactionV2     => jsv.as[SignedSponsorFeeRequestV2].toTx
          case PermitTransactionV1         => jsv.as[SignedPermitRequestV1].toTx
          case PermitTransactionV2         => jsv.as[SignedPermitRequestV2].toTx
          case ExchangeTransactionV2       => jsv.as[SignedExchangeRequestV2].toTx
          case RegisterNodeTransactionV1   => jsv.as[SignedRegisterNodeRequest].toTx
          case RegisterNodeTransactionV2   => jsv.as[SignedRegisterNodeRequestV2].toTx

          case CreateContractTransactionV1  => jsv.as[SignedCreateContractRequestV1].toTx
          case CreateContractTransactionV2  => jsv.as[SignedCreateContractRequestV2].toTx
          case CreateContractTransactionV3  => jsv.as[SignedCreateContractRequestV3].toTx
          case CreateContractTransactionV4  => jsv.as[SignedCreateContractRequestV4].toTx
          case CreateContractTransactionV5  => jsv.as[SignedCreateContractRequestV5].toTx
          case CreateContractTransactionV6  => jsv.as[SignedCreateContractRequestV6].toTx
          case CreateContractTransactionV7  => jsv.as[SignedCreateContractRequestV7].toTx
          case CallContractTransactionV1    => jsv.as[SignedCallContractRequestV1].toTx
          case CallContractTransactionV2    => jsv.as[SignedCallContractRequestV2].toTx
          case CallContractTransactionV3    => jsv.as[SignedCallContractRequestV3].toTx
          case CallContractTransactionV4    => jsv.as[SignedCallContractRequestV4].toTx
          case CallContractTransactionV5    => jsv.as[SignedCallContractRequestV5].toTx
          case CallContractTransactionV7    => jsv.as[SignedCallContractRequestV7].toTx
          case DisableContractTransactionV1 => jsv.as[SignedDisableContractRequestV1].toTx
          case DisableContractTransactionV2 => jsv.as[SignedDisableContractRequestV2].toTx
          case DisableContractTransactionV3 => jsv.as[SignedDisableContractRequestV3].toTx
          case UpdateContractTransactionV1  => jsv.as[SignedUpdateContractRequestV1].toTx
          case UpdateContractTransactionV2  => jsv.as[SignedUpdateContractRequestV2].toTx
          case UpdateContractTransactionV3  => jsv.as[SignedUpdateContractRequestV3].toTx
          case UpdateContractTransactionV4  => jsv.as[SignedUpdateContractRequestV4].toTx
          case UpdateContractTransactionV5  => jsv.as[SignedUpdateContractRequestV5].toTx
          case UpdateContractTransactionV6  => jsv.as[SignedUpdateContractRequestV6].toTx

          case CreatePolicyTransactionV1   => jsv.as[SignedCreatePolicyRequestV1].toTx
          case CreatePolicyTransactionV2   => jsv.as[SignedCreatePolicyRequestV2].toTx
          case CreatePolicyTransactionV3   => jsv.as[SignedCreatePolicyRequestV3].toTx
          case UpdatePolicyTransactionV1   => jsv.as[SignedUpdatePolicyRequestV1].toTx
          case UpdatePolicyTransactionV2   => jsv.as[SignedUpdatePolicyRequestV2].toTx
          case UpdatePolicyTransactionV3   => jsv.as[SignedUpdatePolicyRequestV3].toTx
          case PolicyDataHashTransactionV3 => jsv.as[SignedPolicyDataHashRequestV3].toTx

          case AtomicTransactionV1 => jsv.as[SignedAtomicTransactionRequestV1].toTx

          case ExecutedContractTransactionV1 | ExecutedContractTransactionV2 | ExecutedContractTransactionV3 =>
            Left(GenericError("ExecutedContract transaction is not allowed to be broadcasted"))

          case PolicyDataHashTransactionV1 | PolicyDataHashTransactionV2 =>
            Left(GenericError("PolicyDataHash V1/V2 transaction is not allowed to be broadcasted"))
        }
    }
  }

  def fromProto(protoTx: PbTransaction): Either[ValidationError, ProtoSerializableTransaction] = {
    protoTx.transaction match {
      case PbTransaction.Transaction.GenesisTransaction(_) | PbTransaction.Transaction.GenesisRegisterNodeTransaction(_) |
          PbTransaction.Transaction.GenesisPermitTransaction(_) =>
        Left(GenericError(s"Genesis transaction is not allowed to be broadcasted"))
      case PbTransaction.Transaction.ExecutedContractTransaction(_) =>
        Left(GenericError("ExecutedContract transaction is not allowed to be broadcasted"))
      case PbTransaction.Transaction.PolicyDataHashTransaction(_) if protoTx.version < 3 =>
        Left(GenericError("PolicyDataHash V1/V2 transaction is not allowed to be broadcasted"))
      case _ => TxAdapter.fromProto(protoTx)
    }
  }

  def permitTransactionV1(request: PermitRequestV1, wallet: Wallet, time: Time): Either[ValidationError, PermitTransactionV1] =
    for {
      pk <- findPrivateKey(wallet, request)
      txTimestamp = request.timestamp.getOrElse(time.getTimestamp())
      targetAddress <- AddressOrAlias.fromString(request.target).leftMap(ValidationError.fromCryptoError)
      permOp        <- request.mkPermissionOp(txTimestamp)
      tx            <- PermitTransactionV1.selfSigned(pk, targetAddress, txTimestamp, request.fee, permOp)
    } yield tx

  def permitTransactionV1(request: PermitRequestV1, sender: PublicKeyAccount): Either[ValidationError, PermitTransactionV1] =
    for {
      target   <- AddressOrAlias.fromString(request.target).leftMap(ValidationError.fromCryptoError)
      permOp   <- request.mkPermissionOp(0L)
      permitTx <- PermitTransactionV1.create(sender, target, 0L, request.fee, permOp, Proofs.empty)
    } yield permitTx

  def permitTransactionV2(request: PermitRequestV2, wallet: Wallet, time: Time): Either[ValidationError, PermitTransactionV2] =
    for {
      pk <- findPrivateKey(wallet, request)
      txTimestamp = request.timestamp.getOrElse(time.getTimestamp())
      targetAddress <- AddressOrAlias.fromString(request.target).leftMap(ValidationError.fromCryptoError)
      permOp        <- request.mkPermissionOp(txTimestamp)
      tx            <- PermitTransactionV2.selfSigned(pk, targetAddress, txTimestamp, request.fee, permOp, request.atomicBadge)
    } yield tx

  def permitTransactionV2(request: PermitRequestV2, sender: PublicKeyAccount): Either[ValidationError, PermitTransactionV2] =
    for {
      target   <- AddressOrAlias.fromString(request.target).leftMap(ValidationError.fromCryptoError)
      permOp   <- request.mkPermissionOp(0L)
      permitTx <- PermitTransactionV2.create(sender, target, 0L, request.fee, permOp, request.atomicBadge, Proofs.empty)
    } yield permitTx

  def registerNodeTransaction(request: RegisterNodeRequest, wallet: Wallet, time: Time): Either[ValidationError, RegisterNodeTransactionV1] = {
    for {
      sender <- findPrivateKey(wallet, request)
      txTimestamp = request.timestamp.getOrElse(time.getTimestamp())
      targetPubKey <- PublicKeyAccount.fromBase58String(request.targetPubKey).leftMap(ValidationError.fromCryptoError)
      opType       <- OpType.fromStr(request.opType)
      tx           <- RegisterNodeTransactionV1.selfSigned(sender, targetPubKey, request.nodeName, opType, txTimestamp, request.fee)
    } yield tx
  }

  def registerNodeTransaction(request: RegisterNodeRequest, sender: PublicKeyAccount): Either[ValidationError, RegisterNodeTransactionV1] = {
    for {
      targetPubKey <- PublicKeyAccount.fromBase58String(request.targetPubKey).leftMap(ValidationError.fromCryptoError)
      opType       <- OpType.fromStr(request.opType)
      tx           <- RegisterNodeTransactionV1.create(sender, targetPubKey, request.nodeName, opType, 0L, request.fee, Proofs.empty)
    } yield tx
  }

  def registerNodeTransactionV2(request: RegisterNodeRequestV2, wallet: Wallet, time: Time): Either[ValidationError, RegisterNodeTransactionV2] = {
    for {
      sender <- findPrivateKey(wallet, request)
      txTimestamp = request.timestamp.getOrElse(time.getTimestamp())
      targetPubKey <- PublicKeyAccount.fromBase58String(request.targetPubKey).leftMap(ValidationError.fromCryptoError)
      opType       <- OpType.fromStr(request.opType)
      tx           <- RegisterNodeTransactionV2.selfSigned(sender, targetPubKey, request.nodeName, opType, txTimestamp, request.fee, request.atomicBadge)
    } yield tx
  }

  def registerNodeTransactionV2(request: RegisterNodeRequestV2, sender: PublicKeyAccount): Either[ValidationError, RegisterNodeTransactionV2] = {
    for {
      targetPubKey <- PublicKeyAccount.fromBase58String(request.targetPubKey).leftMap(ValidationError.fromCryptoError)
      opType       <- OpType.fromStr(request.opType)
      tx           <- RegisterNodeTransactionV2.create(sender, targetPubKey, request.nodeName, opType, 0L, request.fee, request.atomicBadge, Proofs.empty)
    } yield tx
  }

  def createContractTransactionV1(request: CreateContractRequestV1,
                                  sender: PublicKeyAccount): Either[ValidationError, CreateContractTransactionV1] = {
    CreateContractTransactionV1.create(sender, request.image, request.imageHash, request.contractName, request.params, request.fee, 0, Proofs.empty)
  }

  def createContractTransactionV1(request: CreateContractRequestV1,
                                  wallet: Wallet,
                                  time: Time): Either[ValidationError, CreateContractTransactionV1] = {
    for {
      pk <- findPrivateKey(wallet, request)
      tx <- CreateContractTransactionV1.selfSigned(pk,
                                                   request.image,
                                                   request.imageHash,
                                                   request.contractName,
                                                   request.params,
                                                   request.fee,
                                                   request.timestamp.getOrElse(time.getTimestamp()))
    } yield tx
  }

  def createContractTransactionV2(request: CreateContractRequestV2,
                                  sender: PublicKeyAccount): Either[ValidationError, CreateContractTransactionV2] = {
    for {
      feeAssetId <- request.decodeFeeAssetId()
      tx <- CreateContractTransactionV2.create(sender,
                                               request.image,
                                               request.imageHash,
                                               request.contractName,
                                               request.params,
                                               request.fee,
                                               0,
                                               feeAssetId,
                                               Proofs.empty)
    } yield tx
  }

  def createContractTransactionV2(request: CreateContractRequestV2,
                                  wallet: Wallet,
                                  time: Time): Either[ValidationError, CreateContractTransactionV2] = {
    for {
      pk         <- findPrivateKey(wallet, request)
      feeAssetId <- request.decodeFeeAssetId()
      tx <- CreateContractTransactionV2.selfSigned(
        pk,
        request.image,
        request.imageHash,
        request.contractName,
        request.params,
        request.fee,
        request.timestamp.getOrElse(time.getTimestamp()),
        feeAssetId,
      )
    } yield tx
  }

  def createContractTransactionV3(request: CreateContractRequestV3,
                                  sender: PublicKeyAccount): Either[ValidationError, CreateContractTransactionV3] = {
    for {
      feeAssetId <- request.decodeFeeAssetId()
      tx <- CreateContractTransactionV3.create(sender,
                                               request.image,
                                               request.imageHash,
                                               request.contractName,
                                               request.params,
                                               request.fee,
                                               0,
                                               feeAssetId,
                                               request.atomicBadge,
                                               Proofs.empty)
    } yield tx
  }

  def createContractTransactionV3(request: CreateContractRequestV3,
                                  wallet: Wallet,
                                  time: Time): Either[ValidationError, CreateContractTransactionV3] = {
    for {
      pk         <- findPrivateKey(wallet, request)
      feeAssetId <- request.decodeFeeAssetId()
      tx <- CreateContractTransactionV3.selfSigned(
        pk,
        request.image,
        request.imageHash,
        request.contractName,
        request.params,
        request.fee,
        request.timestamp.getOrElse(time.getTimestamp()),
        feeAssetId,
        request.atomicBadge
      )
    } yield tx
  }

  def createContractTransactionV4(request: CreateContractRequestV4,
                                  sender: PublicKeyAccount): Either[ValidationError, CreateContractTransactionV4] = {
    for {
      feeAssetId <- request.decodeFeeAssetId()
      tx <- CreateContractTransactionV4.create(
        sender,
        request.image,
        request.imageHash,
        request.contractName,
        request.params,
        request.fee,
        0,
        feeAssetId,
        request.atomicBadge,
        request.validationPolicy,
        request.apiVersion,
        Proofs.empty
      )
    } yield tx
  }

  def createContractTransactionV4(request: CreateContractRequestV4,
                                  wallet: Wallet,
                                  time: Time): Either[ValidationError, CreateContractTransactionV4] = {
    for {
      pk         <- findPrivateKey(wallet, request)
      feeAssetId <- request.decodeFeeAssetId()
      tx <- CreateContractTransactionV4.selfSigned(
        pk,
        request.image,
        request.imageHash,
        request.contractName,
        request.params,
        request.fee,
        request.timestamp.getOrElse(time.getTimestamp()),
        feeAssetId,
        request.atomicBadge,
        request.validationPolicy,
        request.apiVersion
      )
    } yield tx
  }

  def createContractTransactionV5(request: CreateContractRequestV5,
                                  sender: PublicKeyAccount): Either[ValidationError, CreateContractTransactionV5] = {
    for {
      feeAssetId <- request.decodeFeeAssetId()
      tx <- CreateContractTransactionV5.create(
        sender,
        request.image,
        request.imageHash,
        request.contractName,
        request.params,
        request.fee,
        0,
        feeAssetId,
        request.atomicBadge,
        request.validationPolicy,
        request.apiVersion,
        request.payments,
        Proofs.empty
      )
    } yield tx
  }

  def createContractTransactionV5(request: CreateContractRequestV5,
                                  wallet: Wallet,
                                  time: Time): Either[ValidationError, CreateContractTransactionV5] = {
    for {
      pk         <- findPrivateKey(wallet, request)
      feeAssetId <- request.decodeFeeAssetId()
      tx <- CreateContractTransactionV5.selfSigned(
        pk,
        request.image,
        request.imageHash,
        request.contractName,
        request.params,
        request.fee,
        request.timestamp.getOrElse(time.getTimestamp()),
        feeAssetId,
        request.atomicBadge,
        request.validationPolicy,
        request.apiVersion,
        request.payments
      )
    } yield tx
  }

  def createContractTransactionV6(
      request: CreateContractRequestV6,
      wallet: Wallet,
      time: Time): Either[ValidationError, CreateContractTransactionV6] = {
    for {
      pk                   <- findPrivateKey(wallet, request)
      feeAssetId           <- request.decodeFeeAssetId()
      groupParticipantsSet <- parseUniqueAddressSet(request.groupParticipants, "Group participants")
      groupOwnersSet       <- parseUniqueAddressSet(request.groupOwners, "Group owners")
      tx <- CreateContractTransactionV6.selfSigned(
        pk,
        request.image,
        request.imageHash,
        request.contractName,
        request.params,
        request.fee,
        request.timestamp.getOrElse(time.getTimestamp()),
        feeAssetId,
        request.atomicBadge,
        request.validationPolicy,
        request.apiVersion,
        request.payments,
        request.isConfidential,
        groupParticipantsSet,
        groupOwnersSet
      )
    } yield tx
  }

  def createContractTransactionV7(request: CreateContractRequestV7,
                                  sender: PublicKeyAccount): Either[ValidationError, CreateContractTransactionV7] = {
    for {
      apiVersion           <- getContractApiVersion(request.storedContract, request.apiVersion)
      feeAssetId           <- request.decodeFeeAssetId()
      groupParticipantsSet <- parseUniqueAddressSet(request.groupParticipants, "Group participants")
      groupOwnersSet       <- parseUniqueAddressSet(request.groupOwners, "Group owners")
      tx <- CreateContractTransactionV7.create(
        sender = sender,
        contractName = request.contractName,
        params = request.params,
        fee = request.fee,
        timestamp = request.timestamp.getOrElse(0),
        feeAssetId = feeAssetId,
        atomicBadge = request.atomicBadge,
        validationPolicy = request.validationPolicy,
        payments = request.payments,
        isConfidential = request.isConfidential,
        groupParticipants = groupParticipantsSet,
        groupOwners = groupOwnersSet,
        storedContract = request.storedContract,
        apiVersion = apiVersion,
        proofs = Proofs.empty
      )
    } yield tx
  }

  def createContractTransactionV6(request: CreateContractRequestV6,
                                  sender: PublicKeyAccount): Either[ValidationError, CreateContractTransactionV6] = {
    for {
      feeAssetId           <- request.decodeFeeAssetId()
      groupParticipantsSet <- parseUniqueAddressSet(request.groupParticipants, "Group participants")
      groupOwnersSet       <- parseUniqueAddressSet(request.groupOwners, "Group owners")
      tx <- CreateContractTransactionV6.create(
        sender,
        request.image,
        request.imageHash,
        request.contractName,
        request.params,
        request.fee,
        0,
        feeAssetId,
        request.atomicBadge,
        request.validationPolicy,
        request.apiVersion,
        request.payments,
        request.isConfidential,
        groupParticipantsSet,
        groupOwnersSet,
        Proofs.empty
      )
    } yield tx
  }

  def createContractTransactionV7(request: CreateContractRequestV7,
                                  wallet: Wallet,
                                  time: Time): Either[ValidationError, CreateContractTransactionV7] = {
    for {
      apiVersion           <- getContractApiVersion(request.storedContract, request.apiVersion)
      pk                   <- findPrivateKey(wallet, request)
      feeAssetId           <- request.decodeFeeAssetId()
      groupParticipantsSet <- parseUniqueAddressSet(request.groupParticipants, "Group participants")
      groupOwnersSet       <- parseUniqueAddressSet(request.groupOwners, "Group owners")
      tx <- CreateContractTransactionV7.selfSigned(
        pk,
        request.contractName,
        request.params,
        request.fee,
        request.timestamp.getOrElse(time.getTimestamp()),
        feeAssetId,
        request.atomicBadge,
        request.validationPolicy,
        apiVersion,
        request.payments,
        request.isConfidential,
        groupParticipantsSet,
        groupOwnersSet,
        request.storedContract
      )
    } yield tx
  }

  def callContractTransactionV1(request: CallContractRequestV1, sender: PublicKeyAccount): Either[ValidationError, CallContractTransactionV1] =
    for {
      contractId <- ByteStr.decodeBase58(request.contractId).fold(_ => Left(InvalidContractId(request.contractId)), Right(_))
      tx         <- CallContractTransactionV1.create(sender, contractId, request.params, request.fee, 0, Proofs.empty)
    } yield tx

  def callContractTransactionV1(request: CallContractRequestV1, wallet: Wallet, time: Time): Either[ValidationError, CallContractTransactionV1] = {
    for {
      pk         <- findPrivateKey(wallet, request)
      contractId <- ByteStr.decodeBase58(request.contractId).fold(_ => Left(InvalidContractId(request.contractId)), Right(_))
      tx         <- CallContractTransactionV1.selfSigned(pk, contractId, request.params, request.fee, request.timestamp.getOrElse(time.getTimestamp()))
    } yield tx
  }

  def callContractTransactionV2(request: CallContractRequestV2, sender: PublicKeyAccount): Either[ValidationError, CallContractTransactionV2] =
    for {
      contractId <- ByteStr.decodeBase58(request.contractId).fold(_ => Left(InvalidContractId(request.contractId)), Right(_))
      tx         <- CallContractTransactionV2.create(sender, contractId, request.params, request.fee, 0, request.contractVersion, Proofs.empty)
    } yield tx

  def callContractTransactionV2(request: CallContractRequestV2, wallet: Wallet, time: Time): Either[ValidationError, CallContractTransactionV2] = {
    for {
      pk         <- findPrivateKey(wallet, request)
      contractId <- ByteStr.decodeBase58(request.contractId).fold(_ => Left(InvalidContractId(request.contractId)), Right(_))
      tx <- CallContractTransactionV2.selfSigned(pk,
                                                 contractId,
                                                 request.params,
                                                 request.fee,
                                                 request.timestamp.getOrElse(time.getTimestamp()),
                                                 request.contractVersion)
    } yield tx
  }

  def callContractTransactionV3(request: CallContractRequestV3, sender: PublicKeyAccount): Either[ValidationError, CallContractTransactionV3] =
    for {
      contractId <- ByteStr.decodeBase58(request.contractId).fold(_ => Left(InvalidContractId(request.contractId)), Right(_))
      feeAssetId <- request.decodeFeeAssetId()
      tx         <- CallContractTransactionV3.create(sender, contractId, request.params, request.fee, 0, request.contractVersion, feeAssetId, Proofs.empty)
    } yield tx

  def callContractTransactionV3(request: CallContractRequestV3, wallet: Wallet, time: Time): Either[ValidationError, CallContractTransactionV3] = {
    for {
      pk         <- findPrivateKey(wallet, request)
      contractId <- ByteStr.decodeBase58(request.contractId).fold(_ => Left(InvalidContractId(request.contractId)), Right(_))
      feeAssetId <- request.decodeFeeAssetId()
      tx <- CallContractTransactionV3.selfSigned(pk,
                                                 contractId,
                                                 request.params,
                                                 request.fee,
                                                 request.timestamp.getOrElse(time.getTimestamp()),
                                                 request.contractVersion,
                                                 feeAssetId)
    } yield tx
  }

  def callContractTransactionV4(request: CallContractRequestV4, sender: PublicKeyAccount): Either[ValidationError, CallContractTransactionV4] =
    for {
      contractId <- ByteStr.decodeBase58(request.contractId).fold(_ => Left(InvalidContractId(request.contractId)), Right(_))
      feeAssetId <- request.decodeFeeAssetId()
      tx <- CallContractTransactionV4.create(sender,
                                             contractId,
                                             request.params,
                                             request.fee,
                                             0,
                                             request.contractVersion,
                                             feeAssetId,
                                             request.atomicBadge,
                                             Proofs.empty)
    } yield tx

  def callContractTransactionV4(request: CallContractRequestV4, wallet: Wallet, time: Time): Either[ValidationError, CallContractTransactionV4] = {
    for {
      pk         <- findPrivateKey(wallet, request)
      contractId <- ByteStr.decodeBase58(request.contractId).fold(_ => Left(InvalidContractId(request.contractId)), Right(_))
      feeAssetId <- request.decodeFeeAssetId()
      tx <- CallContractTransactionV4.selfSigned(pk,
                                                 contractId,
                                                 request.params,
                                                 request.fee,
                                                 request.timestamp.getOrElse(time.getTimestamp()),
                                                 request.contractVersion,
                                                 feeAssetId,
                                                 request.atomicBadge)
    } yield tx
  }

  def callContractTransactionV5(request: CallContractRequestV5, sender: PublicKeyAccount): Either[ValidationError, CallContractTransactionV5] =
    for {
      contractId <- ByteStr.decodeBase58(request.contractId).fold(_ => Left(InvalidContractId(request.contractId)), Right(_))
      feeAssetId <- request.decodeFeeAssetId()
      tx <- CallContractTransactionV5.create(sender,
                                             contractId,
                                             request.params,
                                             request.fee,
                                             0,
                                             request.contractVersion,
                                             feeAssetId,
                                             request.atomicBadge,
                                             request.payments,
                                             Proofs.empty)
    } yield tx

  def callContractTransactionV5(request: CallContractRequestV5, wallet: Wallet, time: Time): Either[ValidationError, CallContractTransactionV5] = {
    for {
      pk         <- findPrivateKey(wallet, request)
      contractId <- ByteStr.decodeBase58(request.contractId).fold(_ => Left(InvalidContractId(request.contractId)), Right(_))
      feeAssetId <- request.decodeFeeAssetId()
      tx <- CallContractTransactionV5.selfSigned(
        pk,
        contractId,
        request.params,
        request.fee,
        request.timestamp.getOrElse(time.getTimestamp()),
        request.contractVersion,
        feeAssetId,
        request.atomicBadge,
        request.payments
      )
    } yield tx
  }

  def callContractTransactionV7(request: CallContractRequestV7, sender: PublicKeyAccount): Either[ValidationError, CallContractTransactionV7] =
    for {
      contractId      <- ByteStr.decodeBase58(request.contractId).fold(_ => Left(InvalidContractId(request.contractId)), Right(_))
      feeAssetId      <- request.decodeFeeAssetId()
      inputCommitment <- request.inputCommitment.traverse((Commitment.fromBase58 _).andThen(_.leftMap(ValidationError.fromCryptoError)))
      tx <- CallContractTransactionV7.create(
        sender,
        contractId,
        request.params,
        request.fee,
        0,
        request.contractVersion,
        feeAssetId,
        request.atomicBadge,
        request.payments,
        inputCommitment,
        request.contractEngine,
        request.callFunc,
        Proofs.empty
      )
    } yield tx

  def callContractTransactionV7(request: CallContractRequestV7, wallet: Wallet, time: Time): Either[ValidationError, CallContractTransactionV7] = {
    for {
      pk              <- findPrivateKey(wallet, request)
      contractId      <- ByteStr.decodeBase58(request.contractId).fold(_ => Left(InvalidContractId(request.contractId)), Right(_))
      feeAssetId      <- request.decodeFeeAssetId()
      inputCommitment <- request.inputCommitment.traverse((Commitment.fromBase58 _).andThen(_.leftMap(ValidationError.fromCryptoError)))
      tx <- CallContractTransactionV7.selfSigned(
        pk,
        contractId,
        request.params,
        request.fee,
        request.timestamp.getOrElse(time.getTimestamp()),
        request.contractVersion,
        feeAssetId,
        request.atomicBadge,
        request.payments,
        inputCommitment,
        request.contractEngine,
        request.callFunc
      )
    } yield tx
  }

  def disableContractTransactionV1(request: DisableContractRequestV1,
                                   sender: PublicKeyAccount): Either[ValidationError, DisableContractTransactionV1] =
    for {
      contractId <- ByteStr.decodeBase58(request.contractId).fold(_ => Left(InvalidContractId(request.contractId)), Right(_))
      tx         <- DisableContractTransactionV1.create(sender, contractId, request.fee, 0, Proofs.empty)
    } yield tx

  def disableContractTransactionV1(request: DisableContractRequestV1,
                                   wallet: Wallet,
                                   time: Time): Either[ValidationError, DisableContractTransactionV1] = {
    for {
      pk         <- findPrivateKey(wallet, request)
      contractId <- ByteStr.decodeBase58(request.contractId).fold(_ => Left(InvalidContractId(request.contractId)), Right(_))
      tx         <- DisableContractTransactionV1.selfSigned(pk, contractId, request.fee, request.timestamp.getOrElse(time.getTimestamp()))
    } yield tx
  }

  def disableContractTransactionV2(request: DisableContractRequestV2,
                                   sender: PublicKeyAccount): Either[ValidationError, DisableContractTransactionV2] =
    for {
      contractId <- ByteStr.decodeBase58(request.contractId).fold(_ => Left(InvalidContractId(request.contractId)), Right(_))
      feeAssetId <- request.decodeFeeAssetId()
      tx         <- DisableContractTransactionV2.create(sender, contractId, request.fee, 0, feeAssetId, Proofs.empty)
    } yield tx

  def disableContractTransactionV2(request: DisableContractRequestV2,
                                   wallet: Wallet,
                                   time: Time): Either[ValidationError, DisableContractTransactionV2] = {
    for {
      pk         <- findPrivateKey(wallet, request)
      contractId <- ByteStr.decodeBase58(request.contractId).fold(_ => Left(InvalidContractId(request.contractId)), Right(_))
      feeAssetId <- request.decodeFeeAssetId()
      tx         <- DisableContractTransactionV2.selfSigned(pk, contractId, request.fee, request.timestamp.getOrElse(time.getTimestamp()), feeAssetId)
    } yield tx
  }

  def disableContractTransactionV3(request: DisableContractRequestV3,
                                   sender: PublicKeyAccount): Either[ValidationError, DisableContractTransactionV3] =
    for {
      contractId <- ByteStr.decodeBase58(request.contractId).fold(_ => Left(InvalidContractId(request.contractId)), Right(_))
      feeAssetId <- request.decodeFeeAssetId()
      tx         <- DisableContractTransactionV3.create(sender, contractId, request.fee, 0, feeAssetId, request.atomicBadge, Proofs.empty)
    } yield tx

  def disableContractTransactionV3(request: DisableContractRequestV3,
                                   wallet: Wallet,
                                   time: Time): Either[ValidationError, DisableContractTransactionV3] = {
    for {
      pk         <- findPrivateKey(wallet, request)
      contractId <- ByteStr.decodeBase58(request.contractId).fold(_ => Left(InvalidContractId(request.contractId)), Right(_))
      feeAssetId <- request.decodeFeeAssetId()
      tx <- DisableContractTransactionV3.selfSigned(pk,
                                                    contractId,
                                                    request.fee,
                                                    request.timestamp.getOrElse(time.getTimestamp()),
                                                    feeAssetId,
                                                    request.atomicBadge)
    } yield tx
  }

  def updateContractTransactionV1(request: UpdateContractRequestV1, sender: PublicKeyAccount): Either[ValidationError, UpdateContractTransactionV1] =
    for {
      contractId <- ByteStr.decodeBase58(request.contractId).fold(_ => Left(InvalidContractId(request.contractId)), Right(_))
      tx         <- UpdateContractTransactionV1.create(sender, contractId, request.image, request.imageHash, request.fee, 0, Proofs.empty)
    } yield tx

  def updateContractTransactionV1(request: UpdateContractRequestV1,
                                  wallet: Wallet,
                                  time: Time): Either[ValidationError, UpdateContractTransactionV1] = {
    for {
      pk         <- findPrivateKey(wallet, request)
      contractId <- ByteStr.decodeBase58(request.contractId).fold(_ => Left(InvalidContractId(request.contractId)), Right(_))
      tx <- UpdateContractTransactionV1.selfSigned(pk,
                                                   contractId,
                                                   request.image,
                                                   request.imageHash,
                                                   request.fee,
                                                   request.timestamp.getOrElse(time.getTimestamp()))
    } yield tx
  }

  def updateContractTransactionV2(request: UpdateContractRequestV2, sender: PublicKeyAccount): Either[ValidationError, UpdateContractTransactionV2] =
    for {
      contractId <- ByteStr.decodeBase58(request.contractId).fold(_ => Left(InvalidContractId(request.contractId)), Right(_))
      feeAssetId <- request.decodeFeeAssetId()
      tx         <- UpdateContractTransactionV2.create(sender, contractId, request.image, request.imageHash, request.fee, 0, feeAssetId, Proofs.empty)
    } yield tx

  def updateContractTransactionV2(request: UpdateContractRequestV2,
                                  wallet: Wallet,
                                  time: Time): Either[ValidationError, UpdateContractTransactionV2] = {
    for {
      pk         <- findPrivateKey(wallet, request)
      contractId <- ByteStr.decodeBase58(request.contractId).fold(_ => Left(InvalidContractId(request.contractId)), Right(_))
      feeAssetId <- request.decodeFeeAssetId()
      tx <- UpdateContractTransactionV2.selfSigned(pk,
                                                   contractId,
                                                   request.image,
                                                   request.imageHash,
                                                   request.fee,
                                                   request.timestamp.getOrElse(time.getTimestamp()),
                                                   feeAssetId)
    } yield tx
  }

  def updateContractTransactionV3(request: UpdateContractRequestV3, sender: PublicKeyAccount): Either[ValidationError, UpdateContractTransactionV3] =
    for {
      contractId <- ByteStr.decodeBase58(request.contractId).fold(_ => Left(InvalidContractId(request.contractId)), Right(_))
      feeAssetId <- request.decodeFeeAssetId()
      tx <- UpdateContractTransactionV3.create(sender,
                                               contractId,
                                               request.image,
                                               request.imageHash,
                                               request.fee,
                                               0,
                                               feeAssetId,
                                               request.atomicBadge,
                                               Proofs.empty)
    } yield tx

  def updateContractTransactionV3(request: UpdateContractRequestV3,
                                  wallet: Wallet,
                                  time: Time): Either[ValidationError, UpdateContractTransactionV3] = {
    for {
      pk         <- findPrivateKey(wallet, request)
      contractId <- ByteStr.decodeBase58(request.contractId).fold(_ => Left(InvalidContractId(request.contractId)), Right(_))
      feeAssetId <- request.decodeFeeAssetId()
      tx <- UpdateContractTransactionV3.selfSigned(pk,
                                                   contractId,
                                                   request.image,
                                                   request.imageHash,
                                                   request.fee,
                                                   request.timestamp.getOrElse(time.getTimestamp()),
                                                   feeAssetId,
                                                   request.atomicBadge)
    } yield tx
  }

  def updateContractTransactionV4(request: UpdateContractRequestV4, sender: PublicKeyAccount): Either[ValidationError, UpdateContractTransactionV4] =
    for {
      contractId <- ByteStr.decodeBase58(request.contractId).fold(_ => Left(InvalidContractId(request.contractId)), Right(_))
      feeAssetId <- request.decodeFeeAssetId()
      tx <- UpdateContractTransactionV4.create(
        sender,
        contractId,
        request.image,
        request.imageHash,
        request.fee,
        0,
        feeAssetId,
        request.atomicBadge,
        request.validationPolicy,
        request.apiVersion,
        Proofs.empty
      )
    } yield tx

  def updateContractTransactionV4(request: UpdateContractRequestV4,
                                  wallet: Wallet,
                                  time: Time): Either[ValidationError, UpdateContractTransactionV4] = {
    for {
      pk         <- findPrivateKey(wallet, request)
      contractId <- ByteStr.decodeBase58(request.contractId).fold(_ => Left(InvalidContractId(request.contractId)), Right(_))
      feeAssetId <- request.decodeFeeAssetId()
      tx <- UpdateContractTransactionV4.selfSigned(
        pk,
        contractId,
        request.image,
        request.imageHash,
        request.fee,
        request.timestamp.getOrElse(time.getTimestamp()),
        feeAssetId,
        request.atomicBadge,
        request.validationPolicy,
        request.apiVersion
      )
    } yield tx
  }

  def updateContractTransactionV6(request: UpdateContractRequestV6, sender: PublicKeyAccount): Either[ValidationError, UpdateContractTransactionV6] =
    for {
      contractId <- ByteStr.decodeBase58(request.contractId).fold(_ => Left(InvalidContractId(request.contractId)), Right(_))
      feeAssetId <- request.decodeFeeAssetId()
      tx <- UpdateContractTransactionV6.create(
        sender,
        contractId,
        request.fee,
        request.timestamp.getOrElse(0),
        feeAssetId,
        request.atomicBadge,
        request.validationPolicy,
        request.groupParticipants,
        request.groupOwners,
        request.storedContract,
        Proofs.empty,
      )
    } yield tx

  def updateContractTransactionV6(request: UpdateContractRequestV6,
                                  wallet: Wallet,
                                  time: Time): Either[ValidationError, UpdateContractTransactionV6] = {
    for {
      pk         <- findPrivateKey(wallet, request)
      feeAssetId <- request.decodeFeeAssetId()
      contractId <- ByteStr.decodeBase58(request.contractId).fold(_ => Left(InvalidContractId(request.contractId)), Right(_))
      tx <- UpdateContractTransactionV6.selfSigned(
        pk,
        contractId,
        request.fee,
        request.timestamp.getOrElse(time.getTimestamp()),
        feeAssetId,
        request.atomicBadge,
        request.validationPolicy,
        request.groupParticipants,
        request.groupOwners,
        request.storedContract
      )
    } yield tx
  }

  def updateContractTransactionV5(request: UpdateContractRequestV5, sender: PublicKeyAccount): Either[ValidationError, UpdateContractTransactionV5] =
    for {
      contractId           <- ByteStr.decodeBase58(request.contractId).fold(_ => Left(InvalidContractId(request.contractId)), Right(_))
      feeAssetId           <- request.decodeFeeAssetId()
      groupParticipantsSet <- parseUniqueAddressSet(request.groupParticipants, "Group participants")
      groupOwnersSet       <- parseUniqueAddressSet(request.groupOwners, "Group owners")
      tx <- UpdateContractTransactionV5.create(
        sender,
        contractId,
        request.image,
        request.imageHash,
        request.fee,
        0,
        feeAssetId,
        request.atomicBadge,
        request.validationPolicy,
        request.apiVersion,
        groupParticipantsSet,
        groupOwnersSet,
        Proofs.empty
      )
    } yield tx

  def updateContractTransactionV5(request: UpdateContractRequestV5,
                                  wallet: Wallet,
                                  time: Time): Either[ValidationError, UpdateContractTransactionV5] = {
    for {
      pk                   <- findPrivateKey(wallet, request)
      contractId           <- ByteStr.decodeBase58(request.contractId).fold(_ => Left(InvalidContractId(request.contractId)), Right(_))
      feeAssetId           <- request.decodeFeeAssetId()
      groupParticipantsSet <- parseUniqueAddressSet(request.groupParticipants, "Group participants")
      groupOwnersSet       <- parseUniqueAddressSet(request.groupOwners, "Group owners")
      tx <- UpdateContractTransactionV5.selfSigned(
        pk,
        contractId,
        request.image,
        request.imageHash,
        request.fee,
        request.timestamp.getOrElse(time.getTimestamp()),
        feeAssetId,
        request.atomicBadge,
        request.validationPolicy,
        request.apiVersion,
        groupParticipantsSet,
        groupOwnersSet
      )
    } yield tx
  }

  def createPolicyTransactionV1(request: CreatePolicyRequestV1, wallet: Wallet, time: Time): Either[ValidationError, CreatePolicyTransactionV1] = {
    for {
      _      <- Either.cond(request.policyName.length <= PolicyValidation.MaxPolicyNameLength, (), GenericError("policy name is too long"))
      sender <- findPrivateKey(wallet, request)
      txTimestamp = request.timestamp.getOrElse(time.getTimestamp())
      /*_*/
      parsedRecipients <- request.recipients.toList.traverse(s => Address.fromString(s).leftMap(ValidationError.fromCryptoError))
      parsedOwners     <- request.owners.toList.traverse(s => Address.fromString(s).leftMap(ValidationError.fromCryptoError))
      /*_*/
      tx <- CreatePolicyTransactionV1.selfSigned(sender,
                                                 request.policyName,
                                                 request.description,
                                                 parsedRecipients,
                                                 parsedOwners,
                                                 txTimestamp,
                                                 request.fee)
    } yield tx
  }

  def createPolicyTransactionV1(request: CreatePolicyRequestV1, sender: PublicKeyAccount): Either[ValidationError, CreatePolicyTransactionV1] = {
    for {
      _ <- Either.cond(request.policyName.length <= PolicyValidation.MaxPolicyNameLength, (), GenericError("policy name is too long"))
      /*_*/
      parsedRecipients <- request.recipients.toList.traverse(s => Address.fromString(s).leftMap(ValidationError.fromCryptoError))
      parsedOwners     <- request.owners.toList.traverse(s => Address.fromString(s).leftMap(ValidationError.fromCryptoError))
      /*_*/
      tx <- CreatePolicyTransactionV1.create(sender,
                                             request.policyName,
                                             request.description,
                                             parsedRecipients,
                                             parsedOwners,
                                             0L,
                                             request.fee,
                                             Proofs.empty)
    } yield tx
  }

  def createPolicyTransactionV2(request: CreatePolicyRequestV2, wallet: Wallet, time: Time): Either[ValidationError, CreatePolicyTransactionV2] = {
    for {
      _      <- Either.cond(request.policyName.length <= PolicyValidation.MaxPolicyNameLength, (), GenericError("policy name is too long"))
      sender <- findPrivateKey(wallet, request)
      txTimestamp = request.timestamp.getOrElse(time.getTimestamp())
      /*_*/
      parsedRecipients <- request.recipients.toList.traverse(s => Address.fromString(s).leftMap(ValidationError.fromCryptoError))
      parsedOwners     <- request.owners.toList.traverse(s => Address.fromString(s).leftMap(ValidationError.fromCryptoError))
      /*_*/
      tx <- CreatePolicyTransactionV2.selfSigned(
        sender,
        request.policyName,
        request.description,
        parsedRecipients,
        parsedOwners,
        txTimestamp,
        request.fee,
        request.feeAssetId.map(s => ByteStr.decodeBase58(s).get)
      )
    } yield tx
  }

  def createPolicyTransactionV2(request: CreatePolicyRequestV2, sender: PublicKeyAccount): Either[ValidationError, CreatePolicyTransactionV2] = {
    for {
      _ <- Either.cond(request.policyName.length <= PolicyValidation.MaxPolicyNameLength, (), GenericError("policy name is too long"))
      /*_*/
      parsedRecipients <- request.recipients.toList.traverse(s => Address.fromString(s).leftMap(ValidationError.fromCryptoError))
      parsedOwners     <- request.owners.toList.traverse(s => Address.fromString(s).leftMap(ValidationError.fromCryptoError))
      /*_*/
      tx <- CreatePolicyTransactionV2.create(
        sender,
        request.policyName,
        request.description,
        parsedRecipients,
        parsedOwners,
        0L,
        request.fee,
        request.feeAssetId.map(s => ByteStr.decodeBase58(s).get),
        Proofs.empty
      )
    } yield tx
  }

  def createPolicyTransactionV3(request: CreatePolicyRequestV3, wallet: Wallet, time: Time): Either[ValidationError, CreatePolicyTransactionV3] = {
    for {
      _      <- Either.cond(request.policyName.length <= PolicyValidation.MaxPolicyNameLength, (), GenericError("policy name is too long"))
      sender <- findPrivateKey(wallet, request)
      txTimestamp = request.timestamp.getOrElse(time.getTimestamp())
      /*_*/
      parsedRecipients <- request.recipients.toList.traverse(s => Address.fromString(s).leftMap(ValidationError.fromCryptoError))
      parsedOwners     <- request.owners.toList.traverse(s => Address.fromString(s).leftMap(ValidationError.fromCryptoError))
      /*_*/
      tx <- CreatePolicyTransactionV3.selfSigned(
        sender,
        request.policyName,
        request.description,
        parsedRecipients,
        parsedOwners,
        txTimestamp,
        request.fee,
        request.feeAssetId.map(s => ByteStr.decodeBase58(s).get),
        request.atomicBadge
      )
    } yield tx
  }

  def createPolicyTransactionV3(request: CreatePolicyRequestV3, sender: PublicKeyAccount): Either[ValidationError, CreatePolicyTransactionV3] = {
    for {
      _ <- Either.cond(request.policyName.length <= PolicyValidation.MaxPolicyNameLength, (), GenericError("policy name is too long"))
      /*_*/
      parsedRecipients <- request.recipients.toList.traverse(s => Address.fromString(s).leftMap(ValidationError.fromCryptoError))
      parsedOwners     <- request.owners.toList.traverse(s => Address.fromString(s).leftMap(ValidationError.fromCryptoError))
      /*_*/
      tx <- CreatePolicyTransactionV3.create(
        sender,
        request.policyName,
        request.description,
        parsedRecipients,
        parsedOwners,
        0L,
        request.fee,
        request.feeAssetId.map(s => ByteStr.decodeBase58(s).get),
        request.atomicBadge,
        Proofs.empty
      )
    } yield tx
  }

  def updatePolicyTransactionV1(request: UpdatePolicyRequestV1, wallet: Wallet, time: Time): Either[ValidationError, UpdatePolicyTransactionV1] = {
    for {
      sender <- findPrivateKey(wallet, request)
      txTimestamp = request.timestamp.getOrElse(time.getTimestamp())
      /*_*/
      parsedRecipients <- request.recipients.toList.traverse(s => Address.fromString(s).leftMap(ValidationError.fromCryptoError))
      parsedOwners     <- request.owners.toList.traverse(s => Address.fromString(s).leftMap(ValidationError.fromCryptoError))
      /*_*/
      opType          <- OpType.fromStr(request.opType)
      policyIdDecoded <- decodeBase58(request.policyId, s"Failed to decode policyId (${request.policyId}) to Base58")
      tx              <- UpdatePolicyTransactionV1.selfSigned(sender, ByteStr(policyIdDecoded), parsedRecipients, parsedOwners, opType, txTimestamp, request.fee)
    } yield tx
  }

  def updatePolicyTransactionV1(request: UpdatePolicyRequestV1, sender: PublicKeyAccount): Either[ValidationError, UpdatePolicyTransactionV1] = {
    for {
      /*_*/
      parsedRecipients <- request.recipients.toList.traverse(s => Address.fromString(s).leftMap(ValidationError.fromCryptoError))
      parsedOwners     <- request.owners.toList.traverse(s => Address.fromString(s).leftMap(ValidationError.fromCryptoError))
      /*_*/
      opType          <- OpType.fromStr(request.opType)
      policyIdDecoded <- decodeBase58(request.policyId, s"Failed to decode policyId (${request.policyId}) to Base58")
      tx              <- UpdatePolicyTransactionV1.create(sender, ByteStr(policyIdDecoded), parsedRecipients, parsedOwners, opType, 0L, request.fee, Proofs.empty)
    } yield tx
  }

  def updatePolicyTransactionV2(request: UpdatePolicyRequestV2, wallet: Wallet, time: Time): Either[ValidationError, UpdatePolicyTransactionV2] = {
    for {
      sender <- findPrivateKey(wallet, request)
      txTimestamp = request.timestamp.getOrElse(time.getTimestamp())
      /*_*/
      parsedRecipients <- request.recipients.toList.traverse(s => Address.fromString(s).leftMap(ValidationError.fromCryptoError))
      parsedOwners     <- request.owners.toList.traverse(s => Address.fromString(s).leftMap(ValidationError.fromCryptoError))
      /*_*/
      opType          <- OpType.fromStr(request.opType)
      policyIdDecoded <- decodeBase58(request.policyId, s"Failed to decode policyId (${request.policyId}) to Base58")
      parsedFeeAssetId = request.feeAssetId.map(s => ByteStr.decodeBase58(s).get)
      tx <- UpdatePolicyTransactionV2.selfSigned(sender,
                                                 ByteStr(policyIdDecoded),
                                                 parsedRecipients,
                                                 parsedOwners,
                                                 opType,
                                                 txTimestamp,
                                                 request.fee,
                                                 parsedFeeAssetId)
    } yield tx
  }

  def updatePolicyTransactionV2(request: UpdatePolicyRequestV2, sender: PublicKeyAccount): Either[ValidationError, UpdatePolicyTransactionV2] = {
    for {
      parsedRecipients <- request.recipients.toList.traverse(s => Address.fromString(s).leftMap(ValidationError.fromCryptoError))
      parsedOwners     <- request.owners.toList.traverse(s => Address.fromString(s).leftMap(ValidationError.fromCryptoError))
      opType           <- OpType.fromStr(request.opType)
      policyIdDecoded  <- decodeBase58(request.policyId, s"Failed to decode policyId (${request.policyId}) to Base58")
      parsedFeeAssetId = request.feeAssetId.map(s => ByteStr.decodeBase58(s).get)
      tx <- UpdatePolicyTransactionV2.create(sender,
                                             ByteStr(policyIdDecoded),
                                             parsedRecipients,
                                             parsedOwners,
                                             opType,
                                             0L,
                                             request.fee,
                                             parsedFeeAssetId,
                                             Proofs.empty)
    } yield tx
  }

  def updatePolicyTransactionV3(request: UpdatePolicyRequestV3, wallet: Wallet, time: Time): Either[ValidationError, UpdatePolicyTransactionV3] = {
    for {
      sender <- findPrivateKey(wallet, request)
      txTimestamp = request.timestamp.getOrElse(time.getTimestamp())
      /*_*/
      parsedRecipients <- request.recipients.toList.traverse(s => Address.fromString(s).leftMap(ValidationError.fromCryptoError))
      parsedOwners     <- request.owners.toList.traverse(s => Address.fromString(s).leftMap(ValidationError.fromCryptoError))
      /*_*/
      opType          <- OpType.fromStr(request.opType)
      policyIdDecoded <- decodeBase58(request.policyId, s"Failed to decode policyId (${request.policyId}) to Base58")
      parsedFeeAssetId = request.feeAssetId.map(s => ByteStr.decodeBase58(s).get)
      tx <- UpdatePolicyTransactionV3.selfSigned(sender,
                                                 ByteStr(policyIdDecoded),
                                                 parsedRecipients,
                                                 parsedOwners,
                                                 opType,
                                                 txTimestamp,
                                                 request.fee,
                                                 parsedFeeAssetId,
                                                 request.atomicBadge)
    } yield tx
  }

  def updatePolicyTransactionV3(request: UpdatePolicyRequestV3, sender: PublicKeyAccount): Either[ValidationError, UpdatePolicyTransactionV3] = {
    for {
      /*_*/
      parsedRecipients <- request.recipients.toList.traverse(s => Address.fromString(s).leftMap(ValidationError.fromCryptoError))
      parsedOwners     <- request.owners.toList.traverse(s => Address.fromString(s).leftMap(ValidationError.fromCryptoError))
      /*_*/
      opType          <- OpType.fromStr(request.opType)
      policyIdDecoded <- decodeBase58(request.policyId, s"Failed to decode policyId (${request.policyId}) to Base58")
      parsedFeeAssetId = request.feeAssetId.map(s => ByteStr.decodeBase58(s).get)
      tx <- UpdatePolicyTransactionV3.create(sender,
                                             ByteStr(policyIdDecoded),
                                             parsedRecipients,
                                             parsedOwners,
                                             opType,
                                             0L,
                                             request.fee,
                                             parsedFeeAssetId,
                                             request.atomicBadge,
                                             Proofs.empty)
    } yield tx
  }

  def policyDataHashTransactionV3(request: PolicyDataHashRequestV3,
                                  wallet: Wallet,
                                  time: Time): Either[ValidationError, PolicyDataHashTransactionV3] = {
    for {
      sender <- findPrivateKey(wallet, request)
      txTimestamp = request.timestamp.getOrElse(time.getTimestamp())
      policyIdDecoded  <- PrivacyApiService.decodePolicyId(request.policyId)
      parsedDataHash   <- PolicyDataHash.fromBase58String(request.dataHash)
      parsedFeeAssetId <- parseOptionalFeeAssetId(request)
      tx <- PolicyDataHashTransactionV3.selfSigned(sender,
                                                   parsedDataHash,
                                                   policyIdDecoded,
                                                   txTimestamp,
                                                   request.fee,
                                                   parsedFeeAssetId,
                                                   request.atomicBadge)
    } yield tx
  }

  def policyDataHashTransactionV3(request: PolicyDataHashRequestV3,
                                  sender: PublicKeyAccount): Either[ValidationError, PolicyDataHashTransactionV3] = {
    for {
      policyIdDecoded  <- PrivacyApiService.decodePolicyId(request.policyId)
      parsedDataHash   <- PolicyDataHash.fromBase58String(request.dataHash)
      parsedFeeAssetId <- parseOptionalFeeAssetId(request)
    } yield PolicyDataHashTransactionV3(sender, parsedDataHash, policyIdDecoded, 0L, request.fee, parsedFeeAssetId, request.atomicBadge, Proofs.empty)
  }

  private def parseOptionalFeeAssetId(request: PolicyDataHashRequestV3): Either[GenericError, Option[AssetId]] = {
    request.feeAssetId
      .map { feeAssetIdString =>
        ByteStr
          .decodeBase58(feeAssetIdString)
          .toEither
          .bimap(err => GenericError(err.toString), Option(_))
      }
      .getOrElse(Right(None))
  }

  def atomicTransactionV1(request: AtomicTransactionRequestV1, sender: PublicKeyAccount): Either[ValidationError, AtomicTransactionV1] = {
    for {
      innerTxs <- request.transactions.traverse(txJson => SignedAtomicTransactionRequestV1.parseInnerTx(txJson))
      tx       <- AtomicTransactionV1.create(sender, None, innerTxs, 0, Proofs.empty)
    } yield tx

  }

  def atomicTransactionV1(request: AtomicTransactionRequestV1, wallet: Wallet, time: Time): Either[ValidationError, AtomicTransactionV1] = {
    for {
      pk       <- findPrivateKey(wallet, request)
      innerTxs <- request.transactions.traverse(txJson => SignedAtomicTransactionRequestV1.parseInnerTx(txJson))
      tx       <- AtomicTransactionV1.selfSigned(pk, None, innerTxs, request.timestamp.getOrElse(time.getTimestamp()))
    } yield tx
  }

  private def decodeTransferAttachment(attachment: Option[String]): Either[ValidationError, Array[Byte]] = {
    attachment
      .filter(_.nonEmpty)
      .map(value => decodeBase58(value, s"Invalid Base58 attachment string '$value'"))
      .getOrElse(Right(Array.emptyByteArray))
  }

  private def decodeBase58(input: String, errorMessage: => String): Either[ValidationError, Array[Byte]] = {
    Base58.decode(input).toEither.leftMap(_ => GenericError(errorMessage))
  }

  def parseUniqueAddressSet(base58Addresses: List[String], addressCollectionName: String): Either[ValidationError, Set[Address]] = {
    for {
      addressSet <- base58Addresses.traverse(addressFromBase58).map(_.toSet)
      _ <- Either.cond(
        addressSet.size == base58Addresses.size,
        (),
        ValidationError.GenericError(s"$addressCollectionName must be a set of unique addresses")
      )
    } yield addressSet

  }
  def addressFromBase58(base58: String): Either[ValidationError, Address] = {
    Address.fromString(base58).leftMap(ValidationError.fromCryptoError)
  }

}
