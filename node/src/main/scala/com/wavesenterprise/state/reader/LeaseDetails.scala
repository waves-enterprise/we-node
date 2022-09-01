package com.wavesenterprise.state.reader

import com.wavesenterprise.account.{AddressOrAlias, PublicKeyAccount}

case class LeaseDetails(sender: PublicKeyAccount, recipient: AddressOrAlias, height: Int, amount: Long, isActive: Boolean)
