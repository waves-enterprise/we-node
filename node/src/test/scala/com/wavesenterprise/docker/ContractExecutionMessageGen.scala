package com.wavesenterprise.docker

import com.wavesenterprise.state.ByteStr
import com.wavesenterprise.transaction.WithSenderAndRecipient
import org.apache.commons.lang3.RandomStringUtils
import org.scalacheck.Gen
import org.scalatest.Suite

/**
  * [[ContractExecutionMessage]] generation trait
  */
trait ContractExecutionMessageGen extends WithSenderAndRecipient { _: Suite =>

  def messageGen(statuses: Seq[ContractExecutionStatus] = ContractExecutionStatus.values): Gen[ContractExecutionMessage] =
    for {
      sender    <- accountGen
      txId      <- bytes32gen.map(ByteStr(_))
      status    <- Gen.oneOf(statuses)
      code      <- Gen.option(Gen.choose(1, 255))
      message   <- Gen.choose(1, 1000).map(RandomStringUtils.randomAlphabetic)
      timestamp <- ntpTimestampGen
    } yield ContractExecutionMessage(sender, txId, status, code, message, timestamp)

}
