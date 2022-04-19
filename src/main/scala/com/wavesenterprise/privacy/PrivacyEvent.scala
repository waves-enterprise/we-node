package com.wavesenterprise.privacy

import com.wavesenterprise.protobuf.service.privacy.{PrivacyEvent => PbPrivacyEvent}

sealed trait PrivacyEvent {
  def policyId: String
  def dataHash: String
  def timestamp: Long
  protected def eventType: PbPrivacyEvent.EventType

  def toProto: PbPrivacyEvent = {
    PbPrivacyEvent(policyId, dataHash, eventType)
  }
}

case class DataAcquiredEvent(policyId: String, dataHash: String, timestamp: Long) extends PrivacyEvent {
  override protected val eventType: PbPrivacyEvent.EventType = PbPrivacyEvent.EventType.DATA_ACQUIRED
}

case class DataInvalidatedEvent(policyId: String, dataHash: String, timestamp: Long) extends PrivacyEvent {
  override protected val eventType: PbPrivacyEvent.EventType = PbPrivacyEvent.EventType.DATA_INVALIDATED
}
