package com.wavesenterprise.state.diffs.docker

import com.wavesenterprise.crypto.util.Sha256Hash
import com.wavesenterprise.docker.ContractInfo
import com.wavesenterprise.docker.StoredContract.WasmContract
import com.wavesenterprise.transaction.ValidationError
import com.wavesenterprise.wasm.core.WASMExecutor
import scorex.util.encode.Base16

trait BytecodeValidator {

  def checkBytecode(contractInfo: ContractInfo): Either[ValidationError, Unit] = {

    contractInfo.storedContract match {
      case WasmContract(bytecode, bytecodeHash) =>
        val expectedHash = Base16.encode(Sha256Hash().update(bytecode).result())

        for {
          _ <- Either.cond(
            expectedHash == bytecodeHash,
            (),
            ValidationError.InvalidHash(
              s"Invalid bytecodeHash for contract ${contractInfo.contractId}." +
                s"Actual: $bytecodeHash, expected: $expectedHash"
            )
          )
          _ <- Either.cond(
            new WASMExecutor().validateBytecode(bytecode) == 0,
            (),
            ValidationError.GenericError(
              s"Invalid bytecode with hash $bytecodeHash for contract ${contractInfo.contractId}"
            )
          )
        } yield ()

      case _ => Right(())
    }

  }

}
