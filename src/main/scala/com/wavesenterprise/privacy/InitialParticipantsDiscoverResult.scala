package com.wavesenterprise.privacy

import com.wavesenterprise.account.{Address, PublicKeyAccount}

sealed trait InitialParticipantsDiscoverResult {
  def participantPubKey(address: Address): Option[PublicKeyAccount]
}

object InitialParticipantsDiscoverResult {

  object NotNeeded extends InitialParticipantsDiscoverResult {
    override def participantPubKey(address: Address): Option[PublicKeyAccount] = None
  }

  case class Participants(value: Map[Address, PublicKeyAccount]) extends InitialParticipantsDiscoverResult {
    override def participantPubKey(address: Address): Option[PublicKeyAccount] = {
      value.get(address)
    }
  }

}
