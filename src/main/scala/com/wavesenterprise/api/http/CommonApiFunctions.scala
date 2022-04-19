package com.wavesenterprise.api.http

import akka.http.scaladsl.server.Directive1
import com.wavesenterprise.api.http.ApiError.{BlockDoesNotExist, InvalidSignature}
import com.wavesenterprise.state.{Blockchain, ByteStr}
import com.wavesenterprise.block.Block
import com.wavesenterprise.transaction.TransactionParsers

trait CommonApiFunctions { this: ApiRoute =>
  protected[api] def withBlock(blockchain: Blockchain, encodedSignature: String): Directive1[Block] =
    if (encodedSignature.length > TransactionParsers.SignatureStringLength) complete(InvalidSignature)
    else {
      ByteStr
        .decodeBase58(encodedSignature)
        .toOption
        .toRight(InvalidSignature)
        .flatMap(s => blockchain.blockById(s).toRight(BlockDoesNotExist(Left(s)))) match {
        case Right(b) => provide(b)
        case Left(e)  => complete(e)
      }
    }
}
