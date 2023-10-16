package com.wavesenterprise.network.contracts

import com.wavesenterprise.crypto.internals.confidentialcontracts.Commitment

case class ConfidentialContractDataId(commitment: Commitment, dataType: ConfidentialDataType)
