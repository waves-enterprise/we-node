package com.wavesenterprise.block

import com.wavesenterprise.account.PublicKeyAccount
import com.wavesenterprise.consensus.ConsensusBlockData
import com.wavesenterprise.state.ByteStr

class BlockFields(val timestamp: Long,
                  val version: Byte,
                  val reference: ByteStr,
                  val signerData: SignerData,
                  val consensusData: ConsensusBlockData,
                  val featureVotes: Set[Short],
                  val genesisDataOpt: Option[GenesisData]) {

  def sender: PublicKeyAccount = signerData.generator
  def uniqueId: ByteStr        = signerData.signature
}
