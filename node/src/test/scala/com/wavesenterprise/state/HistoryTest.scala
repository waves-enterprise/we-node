package com.wavesenterprise.state

import com.wavesenterprise.block.Block
import com.wavesenterprise.crypto.SignatureLength
import com.wavesenterprise.lagonaki.mocks.TestBlock

trait HistoryTest {
  val genesisBlock: Block = TestBlock.withReference(ByteStr(Array.fill(SignatureLength)(0: Byte)))

  def getNextTestBlock(blockchain: Blockchain): Block =
    TestBlock.withReference(blockchain.lastBlock.get.uniqueId)

  def getNextTestBlockWithVotes(blockchain: Blockchain, votes: Set[Short]): Block =
    TestBlock.withReferenceAndFeatures(blockchain.lastBlock.get.uniqueId, votes)
}
