package com.wavesenterprise.privacy.db

import com.wavesenterprise.state.ByteStr

case class PolicyData(policyId: String, hash: String, data: ByteStr)
