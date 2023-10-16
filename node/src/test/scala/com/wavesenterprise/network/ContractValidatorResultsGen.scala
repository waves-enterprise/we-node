package com.wavesenterprise.network

import com.wavesenterprise.CoreTransactionGen
import com.wavesenterprise.crypto.internals.confidentialcontracts.Commitment
import com.wavesenterprise.state.ByteStr
import com.wavesenterprise.transaction.docker.{FilteredKeys, ReadDescriptor, ReadingsHash, SpecificKeys}
import org.scalacheck.Gen
import org.scalatest.Suite

trait ContractValidatorResultsGen extends CoreTransactionGen { _: Suite =>
  protected val contractValidatorResultsV1Gen: Gen[ContractValidatorResultsV1] = for {
    sender      <- accountGen
    txId        <- bytes32gen.map(ByteStr(_))
    keyBlockId  <- bytes64gen.map(ByteStr(_))
    resultsHash <- bytes32gen.map(ByteStr(_))
  } yield ContractValidatorResultsV1(sender, txId, keyBlockId, resultsHash)

  protected val readingGen: Gen[ReadDescriptor] = Gen.oneOf(0, 1).flatMap {
    case 0 => specificKeysGen
    case 1 => filteredKeysGen
  }

  protected val specificKeysGen: Gen[SpecificKeys] = for {
    contractId <- bytes32gen.map(ByteStr(_))
    n          <- Gen.choose(5, 20)
    keys       <- Gen.listOfN(n, dataAsciiRussianGen)
  } yield SpecificKeys(contractId, keys)

  protected val filteredKeysGen: Gen[FilteredKeys] = for {
    contractId <- bytes32gen.map(ByteStr(_))
    matches    <- Gen.option(dataAsciiRussianGen)
    offset     <- Gen.option(positiveIntGen)
    limit      <- Gen.option(positiveIntGen)
  } yield FilteredKeys(contractId, matches, offset, limit)

  protected val readingsGen: Gen[Seq[ReadDescriptor]] = for {
    n        <- Gen.choose(5, 20)
    readings <- Gen.listOfN(n, readingGen)
  } yield readings

  protected val readingsHashGen: Gen[ReadingsHash]   = bytes32gen.map(arr => ReadingsHash(ByteStr(arr)))
  protected val outputCommitmentGen: Gen[Commitment] = bytes32gen.map(arr => Commitment(ByteStr(arr)))
  protected val contractValidatorResultsV2Gen: Gen[ContractValidatorResultsV2] = for {
    sender                                                          <- accountGen
    ContractValidatorResultsV1(_, txId, keyBlockId, resultsHash, _) <- contractValidatorResultsV1Gen
    readings                                                        <- readingsGen
    readingsHash                                                    <- readingsHashGen
    outputCommitment                                                <- outputCommitmentGen
  } yield ContractValidatorResultsV2(sender, txId, keyBlockId, readings, Some(readingsHash), outputCommitment, resultsHash)
}
