package com.wavesenterprise.privacy

import com.wavesenterprise.account.{Address, PublicKeyAccount}

import java.security.cert.X509Certificate

sealed trait InitialParticipantsDiscoverResult {
  def participantPubKey(address: Address): Option[PublicKeyAccount]
  def participantCertificates(address: Address): List[X509Certificate]
}

object InitialParticipantsDiscoverResult {

  object NotNeeded extends InitialParticipantsDiscoverResult {
    override def participantPubKey(address: Address): Option[PublicKeyAccount] = None

    override def participantCertificates(address: Address): List[X509Certificate] = List.empty
  }

  case class Participants(value: Map[Address, (PublicKeyAccount, List[X509Certificate])]) extends InitialParticipantsDiscoverResult {
    override def participantPubKey(address: Address): Option[PublicKeyAccount] = {
      value.get(address).map { case (pka, _) => pka }
    }

    override def participantCertificates(address: Address): List[X509Certificate] = {
      value.get(address).map { case (_, certificates) => certificates }.toList.flatten
    }
  }

}
