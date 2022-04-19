package com.wavesenterprise.block

import com.wavesenterprise.account.PublicKeyAccount
import com.wavesenterprise.state.ByteStr

case class SignerData(generator: PublicKeyAccount, signature: ByteStr) {
  lazy val generatorAddress = generator.toAddress
}
