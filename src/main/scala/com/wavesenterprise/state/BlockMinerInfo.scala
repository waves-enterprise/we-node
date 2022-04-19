package com.wavesenterprise.state

import com.wavesenterprise.block.Block.BlockId
import com.wavesenterprise.consensus.ConsensusBlockData

case class BlockMinerInfo(consensus: ConsensusBlockData, timestamp: Long, blockId: BlockId)
