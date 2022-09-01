package com.wavesenterprise.database

import com.wavesenterprise.privacy.{PolicyDataHash, PolicyDataId, PrivacyItemDescriptor}
import com.wavesenterprise.state.ByteStr

trait PrivacyState {
  def pendingPrivacyItems(): Set[PolicyDataId]
  def isPending(policyId: ByteStr, dataHash: PolicyDataHash): Boolean
  def addToPending(policyId: ByteStr, dataHash: PolicyDataHash): Boolean
  def addToPending(policyDataIds: Set[PolicyDataId]): Int
  def removeFromPendingAndLost(policyId: ByteStr, dataHash: PolicyDataHash): (Boolean, Boolean)
  def lostPrivacyItems(): Set[PolicyDataId]
  def isLost(policyId: ByteStr, dataHash: PolicyDataHash): Boolean
  def pendingToLost(policyId: ByteStr, dataHash: PolicyDataHash): (Boolean, Boolean)
  def privacyItemDescriptor(policyId: ByteStr, dataHash: PolicyDataHash): Option[PrivacyItemDescriptor]
  def putItemDescriptor(policyId: ByteStr, dataHash: PolicyDataHash, descriptor: PrivacyItemDescriptor): Unit
}
