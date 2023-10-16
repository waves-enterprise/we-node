package com.wavesenterprise.database

import com.wavesenterprise.block.DiscardedBlocks

case class RollbackResult(initialHeight: Int, discardedBlocks: DiscardedBlocks)
