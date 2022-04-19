package com.wavesenterprise.database.snapshot

import cats.implicits._
import com.wavesenterprise.account.Address
import com.wavesenterprise.database.docker.KeysRequest
import com.wavesenterprise.docker.ContractInfo
import com.wavesenterprise.state.ContractBlockchain.ContractReadingContext.Default
import com.wavesenterprise.state.{Blockchain, ByteStr}
import com.wavesenterprise.utils.ScorexLogging

trait SnapshotVerifier extends ScorexLogging {

  protected def state: Blockchain
  protected def snapshot: Blockchain

  protected def verifyAccounts(): Either[VerificationError, Set[Address]] = {
    val accounts = snapshot.accounts()
    Either.cond(accounts == state.accounts(), accounts, VerificationError(s"Accounts are not equal"))
  }

  protected def verifyAssets(): Either[VerificationError, Set[ByteStr]] = {
    val assets = snapshot.assets()
    Either.cond(assets == state.assets(), assets, VerificationError(s"Assets are not equal"))
  }

  protected def verifyContracts(): Either[VerificationError, Set[ContractInfo]] = {
    val contracts = snapshot.contracts()
    Either.cond(contracts == state.contracts(), contracts, VerificationError(s"Contracts are not equal"))
  }

  protected def verifyPolicies(): Either[VerificationError, Set[ByteStr]] = {
    val policies = snapshot.policies()
    Either.cond(policies == state.policies(), policies, VerificationError(s"Policies are not equal"))
  }

  protected def verifyNetworkParticipants(): Either[VerificationError, Set[Address]] = {
    val networkParticipants = snapshot.networkParticipants().toSet
    Either.cond(networkParticipants == state.networkParticipants().toSet,
                networkParticipants,
                VerificationError(s"Network participants are not equal"))
  }

  protected def verifyMiners(): Either[VerificationError, Seq[Address]] = {
    val miners = snapshot.miners.suggestedMiners
    Either.cond(miners == state.miners.suggestedMiners, miners, VerificationError(s"Miners are not equal"))
  }

  protected def verifyContractValidators(): Either[VerificationError, Set[Address]] = {
    val contractValidators = snapshot.contractValidators.suggestedValidators
    Either.cond(contractValidators === state.contractValidators.suggestedValidators,
                contractValidators,
                VerificationError(s"Contract validators are not equal"))
  }

  protected def verifyNonEmptyRoleAddresses(): Either[VerificationError, Set[Address]] = {
    val nonEmptyRoleAddresses = snapshot.allNonEmptyRoleAddresses.toSet
    Either.cond(nonEmptyRoleAddresses == state.allNonEmptyRoleAddresses.toSet,
                nonEmptyRoleAddresses,
                VerificationError(s"Role addresses are not equal"))
  }

  protected def verifyCarryFee(): Either[VerificationError, Unit] = {
    Either.cond(snapshot.carryFee == state.carryFee, (), VerificationError(s"Carry fees are not equal"))
  }

  protected def verifyAccountState(address: Address): Either[VerificationError, Unit] = {
    for {
      _ <- Either.cond(snapshot.portfolio(address) == state.portfolio(address), (), VerificationError(s"Portfolio is not equal for '$address'"))
      _ <- Either.cond(snapshot.balance(address) == state.balance(address), (), VerificationError(s"Balance is not equal for '$address'"))
      _ <- Either.cond(snapshot.hasScript(address) == state.hasScript(address), (), VerificationError(s"'Has script' is not equal for '$address'"))
      _ <- Either.cond(snapshot.accountScript(address) == state.accountScript(address), (), VerificationError(s"Script is not equal for '$address'"))
      _ <- Either.cond(snapshot.permissions(address) == state.permissions(address), (), VerificationError(s"Permissions is not equal for '$address'"))
    } yield ()
  }

  protected def verifyAssetState(asset: ByteStr): Either[VerificationError, Unit] = {
    for {
      _ <- Either.cond(snapshot.asset(asset) == state.asset(asset), (), VerificationError(s"Asset info is not equal for '$asset'"))
      _ <- Either.cond(snapshot.assetDescription(asset) == state.assetDescription(asset),
                       (),
                       VerificationError(s"Asset description is not equal for '$asset'"))
      _ <- Either.cond(snapshot.hasAssetScript(asset) == state.hasAssetScript(asset),
                       (),
                       VerificationError(s"'Has asset script' is not equal for '$asset'"))
      _ <- Either.cond(snapshot.assetScript(asset) == state.assetScript(asset), (), VerificationError(s"Asset script is not equal for '$asset'"))
    } yield ()
  }

  protected def verifyContractKeys(contractId: ByteStr): Either[VerificationError, Vector[String]] = {
    val request = KeysRequest(contractId)
    val keys    = snapshot.contractKeys(request, Default)
    Either.cond(keys.toSet == state.contractKeys(request, Default).toSet, keys, VerificationError(s"Contract keys are not equal for '$contractId'"))
  }

  protected def verifyContractData(contract: ByteStr, key: String): Either[VerificationError, Unit] = {
    Either.cond(
      snapshot.contractData(contract, key, Default) == state.contractData(contract, key, Default),
      (),
      VerificationError(s"Contract data are not equal for '$contract' and key '$key'")
    )
  }

  protected def verifyPolicyState(policy: ByteStr): Either[VerificationError, Unit] = {
    for {
      _ <- Either.cond(snapshot.policyExists(policy) == state.policyExists(policy),
                       (),
                       VerificationError(s"'Policy exists' is not equal for '$policy'"))
      _ <- Either.cond(snapshot.policyOwners(policy) == state.policyOwners(policy),
                       (),
                       VerificationError(s"Policy owners are not equal for '$policy'"))
      _ <- Either.cond(snapshot.policyRecipients(policy) == state.policyRecipients(policy),
                       (),
                       VerificationError(s"Policy recipients are not equal for '$policy'"))
      _ <- Either.cond(snapshot.policyDataHashes(policy) == state.policyDataHashes(policy),
                       (),
                       VerificationError(s"Policy data hashes are not equal for '$policy'"))
    } yield ()
  }
}

case class VerificationError(message: String)

class SyncSnapshotVerifier(override val state: Blockchain, override val snapshot: Blockchain) extends SnapshotVerifier {

  protected def verifyContract(contract: ContractInfo): Either[VerificationError, Unit] = {
    for {
      keys <- verifyContractKeys(contract.contractId)
      _    <- keys.traverse(verifyContractData(contract.contractId, _))
    } yield ()
  }

  protected def verifyAccountData(address: Address): Either[VerificationError, Unit] = {
    Either.cond(snapshot.accountData(address) == state.accountData(address), (), VerificationError(s"Account data is not equal for '$address'"))
  }

  def verify(): Either[VerificationError, Unit] = {
    for {
      accounts  <- verifyAccounts()
      _         <- accounts.toList.traverse(verifyAccountState)
      _         <- accounts.toList.traverse(verifyAccountData)
      assets    <- verifyAssets()
      _         <- assets.toList.traverse(verifyAssetState)
      contracts <- verifyContracts()
      _         <- contracts.toList.traverse(verifyContract)
      policies  <- verifyPolicies()
      _         <- policies.toList.traverse(verifyPolicyState)
      _         <- verifyNetworkParticipants()
      _         <- verifyMiners()
      _         <- verifyContractValidators()
      _         <- verifyNonEmptyRoleAddresses()
      _         <- verifyCarryFee()
    } yield ()
  }
}
