package com.wavesenterprise.history

import com.wavesenterprise.transaction.Transaction

case class MicroInfoForChain(txs: Seq[Transaction], microTimestamp: Long)
