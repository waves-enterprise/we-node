package com.wavesenterprise.generator.utils

import com.wavesenterprise.account.PrivateKeyAccount
import com.wavesenterprise.state.ByteStr

object Universe {
  var AccountsWithBalances: List[(PrivateKeyAccount, Long)] = Nil
  var IssuedAssets: List[ByteStr]                           = Nil
  var Leases: List[ByteStr]                                 = Nil
}
