package com.wavesenterprise.docker

import com.wavesenterprise.docker.StoredContract.DockerContract
import com.wavesenterprise.state.ByteStr
import com.wavesenterprise.transaction.docker.CreateContractTransactionV1
import com.wavesenterprise.transaction.{CommonGen, WithSenderAndRecipient}
import com.wavesenterprise.utils.Base64
import monix.eval.Coeval
import org.apache.commons.codec.digest.DigestUtils
import org.scalacheck.Gen
import org.scalatest.Suite

trait ContractInfoGen extends CommonGen with WithSenderAndRecipient { _: Suite =>

  val dockerContractInfoGen: Gen[ContractInfo] =
    for {
      account          <- accountGen
      id               <- bytes32gen.map(ByteStr(_))
      imageBytes       <- genBoundedString(CreateContractTransactionV1.ImageMinLength, CreateContractTransactionV1.ImageMaxLength)
      imageHash        <- bytes32gen.map(DigestUtils.sha256Hex)
      version          <- Gen.oneOf(1, 2)
      active           <- Gen.oneOf(true, false)
      validationPolicy <- validationPolicyGen
      dockerImage = DockerContract(Base64.encode(imageBytes), imageHash, ContractApiVersion.Current)
    } yield {
      ContractInfo(
        Coeval.evalOnce(account),
        id,
        dockerImage,
        version,
        active,
        validationPolicy
      )
    }
}
