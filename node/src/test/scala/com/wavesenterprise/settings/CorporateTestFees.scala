package com.wavesenterprise.settings

import com.wavesenterprise.transaction.{
  AtomicTransaction,
  CreateAliasTransaction,
  CreatePolicyTransaction,
  DataTransaction,
  GenesisPermitTransaction,
  GenesisTransaction,
  PolicyDataHashTransaction,
  RegisterNodeTransactionV1,
  UpdatePolicyTransaction
}
import com.wavesenterprise.transaction.acl.PermitTransaction
import com.wavesenterprise.transaction.assets.{
  BurnTransaction,
  IssueTransaction,
  ReissueTransaction,
  SetAssetScriptTransactionV1,
  SponsorFeeTransactionV1
}
import com.wavesenterprise.transaction.assets.exchange.ExchangeTransaction
import com.wavesenterprise.transaction.docker.{
  CallContractTransaction,
  CreateContractTransaction,
  DisableContractTransaction,
  ExecutedContractTransaction,
  UpdateContractTransaction
}
import com.wavesenterprise.transaction.lease.{LeaseCancelTransaction, LeaseTransaction}
import com.wavesenterprise.transaction.smart.SetScriptTransaction
import com.wavesenterprise.transaction.transfer.{MassTransferTransaction, TransferTransaction}
import com.wavesenterprise.utils.NumberUtils.DoubleExt

case class CorporateTestFees(base: Map[Byte, WestAmount], additional: Map[Byte, WestAmount]) {
  def forTxType(typeId: Byte): Long           = base(typeId).units
  def forTxTypeAdditional(typeId: Byte): Long = additional(typeId).units
}

object CorporateTestFees {
  val fees: Map[Byte, WestAmount] = Map(
    GenesisTransaction.typeId          -> 0.west,
    GenesisPermitTransaction.typeId    -> 0.west,
    IssueTransaction.typeId            -> 0.west,
    TransferTransaction.typeId         -> 0.west,
    ReissueTransaction.typeId          -> 0.01.west,
    BurnTransaction.typeId             -> 0.01.west,
    ExchangeTransaction.typeId         -> 0.005.west,
    LeaseTransaction.typeId            -> 0.01.west,
    LeaseCancelTransaction.typeId      -> 0.01.west,
    CreateAliasTransaction.typeId      -> 0.west,
    MassTransferTransaction.typeId     -> 0.05.west,
    DataTransaction.typeId             -> 0.west,
    SetScriptTransaction.typeId        -> 0.5.west,
    SponsorFeeTransactionV1.typeId     -> 0.west,
    SetAssetScriptTransactionV1.typeId -> 1.0.west,
    PermitTransaction.typeId           -> 0.01.west,
    CreateContractTransaction.typeId   -> 1.0.west,
    CallContractTransaction.typeId     -> 0.1.west,
    ExecutedContractTransaction.typeId -> 0.0.west,
    DisableContractTransaction.typeId  -> 0.01.west,
    UpdateContractTransaction.typeId   -> 0.0.west,
    RegisterNodeTransactionV1.typeId   -> 0.01.west,
    CreatePolicyTransaction.typeId     -> 1.0.west,
    UpdatePolicyTransaction.typeId     -> 0.5.west,
    PolicyDataHashTransaction.typeId   -> 0.05.west,
    AtomicTransaction.typeId           -> 0.west
  ).mapValues(WestAmount(_))

  val additionalFees: Map[Byte, WestAmount] = Map(
    MassTransferTransaction.typeId -> 0.west,
    DataTransaction.typeId         -> 0.01.west
  ).mapValues(WestAmount(_))

  val defaultFees: TestFees = TestFees(fees, additionalFees)

  def customFees(customFees: Option[(Byte, WestAmount)]): TestFees = {
    customFees match {
      case Some(fee) => TestFees(fees + fee, additionalFees)
      case None      => TestFees(fees, additionalFees)
    }
  }

  def TransferTransactionFee(amount: Long) = TransferTransaction.typeId -> WestAmount(amount)
}
