package com.wavesenterprise.state

import com.wavesenterprise.account.Address
import com.wavesenterprise.privacy.{PolicyDataHash, PolicyDataId}

trait PrivacyBlockchain {

  def policies(): Set[ByteStr]

  def policyExists(policyId: ByteStr): Boolean

  def policyOwners(policyId: ByteStr): Set[Address]

  def policyRecipients(policyId: ByteStr): Set[Address]

  def policyDataHashes(policyId: ByteStr): Set[PolicyDataHash]

  def policyDataHashExists(policyId: ByteStr, dataHash: PolicyDataHash): Boolean

  def policyDataHashTxId(id: PolicyDataId): Option[ByteStr]
}
