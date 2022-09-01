package com.wavesenterprise.transaction

import com.wavesenterprise.block.{Block, MicroBlock}

object BlockchainEventError {
  case class MicroBlockAppendError(err: String, microBlock: MicroBlock) extends ValidationError {
    override def toString: String = s"MicroBlockAppendError($err, $microBlock])"
  }

  case class BlockAppendError(err: String, b: Block) extends ValidationError
}
