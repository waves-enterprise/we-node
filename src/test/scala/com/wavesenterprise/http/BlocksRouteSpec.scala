package com.wavesenterprise.http

import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server.Route
import com.wavesenterprise.TestSchedulers.apiComputationsScheduler
import com.wavesenterprise.account.Address
import com.wavesenterprise.api.http.ApiError.{BlockDoesNotExist, InvalidSignature, TooBigArrayAllocation}
import com.wavesenterprise.api.http.{ApiRoute, BlocksApiRoute}
import com.wavesenterprise.consensus.PoSLikeConsensusBlockData
import com.wavesenterprise.settings.ApiSettings
import com.wavesenterprise.state.{Blockchain, ByteStr}
import com.wavesenterprise.transaction.TransactionParsers
import com.wavesenterprise.{BlockGen, NoShrink, TestTime}
import org.scalacheck.{Arbitrary, Gen}
import org.scalamock.scalatest.MockFactory
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks
import play.api.libs.json.{JsArray, JsObject, Json}

import scala.util.{Random, Try}

class BlocksRouteSpec
    extends RouteSpec("/blocks")
    with ApiSettingsHelper
    with ScalaCheckPropertyChecks
    with BlockGen
    with MockFactory
    with ApiRoute
    with NoShrink {

  private val blockchain: Blockchain        = mock[Blockchain]
  override protected val nodeOwner: Address = accountGen.sample.get.toAddress
  override val time: TestTime               = new TestTime()
  private val blocksApiRoute                = new BlocksApiRoute(restAPISettings, time, blockchain, nodeOwner, apiComputationsScheduler)
  override val route: Route                 = blocksApiRoute.route

  override val settings: ApiSettings = restAPISettings

  routePath("/address/{address}/{from}/{to}") - {
    val gen = for {
      from                  <- positiveIntGen
      to                    <- Gen.choose(from + 1, from + blocksApiRoute.MaxBlocksPerRequest - 1)
      predefinedBlocksCount <- Gen.choose(1, to - from)
      expectedBlocks        <- Gen.listOfN(predefinedBlocksCount, predefinedSignerBlockGen)
      randomBlocks          <- Gen.listOfN(to - from - predefinedBlocksCount + 1, randomSignerBlockGen)
    } yield {
      val expectedJsons        = expectedBlocks.map(_.json())
      val generatedBlocksBytes = Random.shuffle(expectedBlocks ++ randomBlocks).map(_.bytes())
      val address              = expectedBlocks.head.signerData.generatorAddress.address
      (from, to, expectedJsons, generatedBlocksBytes, address)
    }

    "return correct blocks" in {
      forAll(gen) {
        case (from, to, expectedJsons, generatedBlocksBytes, address) =>
          Range.inclusive(from, to).foreach { i =>
            (blockchain.blockBytes(_: Int)).expects(i).returns(Some(generatedBlocksBytes(i - from)))
          }

          Get(routePath(s"/address/$address/$from/$to")) ~> route ~> check {
            response.status shouldBe StatusCodes.OK

            val responseJsons = responseAs[JsArray].value.map(_.as[JsObject] - "height")
            responseJsons.size shouldBe expectedJsons.size
            responseJsons should contain theSameElementsAs expectedJsons
          }
      }
    }

    "return TooBigArrayAllocation" in {
      forAll(addressGen, positiveIntGen) {
        case (address, from) =>
          val to = from + blocksApiRoute.MaxBlocksPerRequest + 1
          Get(routePath(s"/address/$address/$from/$to")) ~> route ~> check {
            response.status shouldBe StatusCodes.BadRequest
            responseAs[JsObject] shouldBe TooBigArrayAllocation.json
          }
      }
    }
  }

  routePath("/child/{signature}") - {
    "return correct successor" in {
      forAll(randomSignerBlockGen, randomSignerBlockGen, positiveIntGen) {
        case (blockA, blockB, height) =>
          (blockchain.blockBytes(_: ByteStr)).expects(blockA.signerData.signature).returns(Some(blockA.bytes()))
          (blockchain.heightOf(_: ByteStr)).expects(blockA.uniqueId).returns(Some(height))
          (blockchain.blockBytes(_: Int)).expects(height + 1).returns(Some(blockB.bytes()))

          Get(routePath(s"/child/${blockA.signerData.signature}")) ~> route ~> check {
            response.status shouldBe StatusCodes.OK
            responseAs[JsObject] shouldBe blockB.json()
          }
      }
    }

    "return error message" in {
      forAll(randomSignerBlockGen, positiveIntGen) {
        case (block, height) =>
          (blockchain.blockBytes(_: ByteStr)).expects(block.signerData.signature).returns(Some(block.bytes()))
          (blockchain.heightOf(_: ByteStr)).expects(block.uniqueId).returns(Some(height))
          (blockchain.blockBytes(_: Int)).expects(height + 1).returns(None)

          Get(routePath(s"/child/${block.signerData.signature}")) ~> route ~> check {
            response.status shouldBe StatusCodes.OK
            responseAs[JsObject] shouldBe Json.obj("status" -> "error", "details" -> "No child blocks")
          }

          (blockchain.blockBytes(_: ByteStr)).expects(block.signerData.signature).returns(None)
          Get(routePath(s"/child/${block.signerData.signature}")) ~> route ~> check {
            response.status shouldBe StatusCodes.NotFound
            responseAs[JsObject] shouldBe BlockDoesNotExist(Left(block.signerData.signature)).json
          }

          val tooBigSignature = ByteStr(Array.fill(TransactionParsers.SignatureStringLength + 1)(0))
          Get(routePath(s"/child/$tooBigSignature")) ~> route ~> check {
            response.status shouldBe StatusCodes.BadRequest
            responseAs[JsObject] shouldBe InvalidSignature.json
          }
      }
    }
  }

  routePath("/height/{signature}") - {
    "return correct height" in {
      forAll(randomSignerBlockGen, positiveIntGen) {
        case (block, height) =>
          (blockchain.heightOf(_: ByteStr)).expects(block.signerData.signature).returns(Some(height))

          val arr = block.signerData.signature.arr
          arr(0) = (arr(0) - 1).toByte
          Get(routePath(s"/height/${ByteStr.apply(arr)}")) ~> route ~> check {
            val resp = responseAs[JsObject]
            println(resp)
            response.status shouldBe StatusCodes.OK
            responseAs[JsObject] shouldBe Json.obj("height" -> height)
          }
      }
    }
  }

  routePath("/at/{height}") - {
    "return correct blocks" in {
      forAll(randomSignerBlockGen, positiveIntGen) {
        case (block, height) =>
          (blockchain.blockBytes(_: Int)).expects(height).returns(Some(block.bytes()))
          Get(routePath(s"/at/$height")) ~> route ~> check {
            response.status shouldBe StatusCodes.OK
            responseAs[JsObject] - "height" shouldBe block.json()
          }
      }
    }
  }

  routePath("/seqext/{from}/{to}") - {
    "return correct blocks seq" in {
      forAll(randomBlocksSeqGen) {
        case (from, to, blocks) =>
          Range
            .inclusive(from, to)
            .foreach(i => {
              (blockchain.blockBytes(_: Int)).expects(i).returns(Some(blocks(i - from).bytes()))
            })
          Get(routePath(s"/seqext/$from/$to")) ~> route ~> check {
            response.status shouldBe StatusCodes.OK
            val blocksJson = responseAs[JsArray]
            blocksJson.value.foreach(json => {
              val height        = (json \ "height").as[Int]
              val block         = blocks(height - from)
              val consensusData = block.consensusData.asInstanceOf[PoSLikeConsensusBlockData]
              (json \ "version").as[Byte] shouldBe block.version
              (json \ "timestamp").as[Long] shouldBe block.timestamp
              (json \ "reference").as[String] shouldBe block.reference.toString
              val consensusDataJson = (json \ "pos-consensus").get.asInstanceOf[JsObject]
              (consensusDataJson \ "base-target").as[Long] shouldBe consensusData.baseTarget
              (consensusDataJson \ "generation-signature").as[String] shouldBe consensusData.generationSignature.base58
              (json \ "generator").as[String] shouldBe block.signerData.generator.toString
              (json \ "signature").as[String] shouldBe block.signerData.signature.base58
              (json \ "blocksize").as[Int] shouldBe block.bytes().length
              (json \ "transactionCount").as[Int] shouldBe block.blockHeader.transactionCount
              (json \ "fee").as[Long] should be >= 0L
              (json \ "transactions").get.isInstanceOf[JsArray] shouldBe true
            })
          }
      }
    }
  }

  routePath("{path with positive int path components}") - {
    "return Bad Request on not positive integers" in {
      def pathBuilder(constantPath: String, countArgs: Int)(pathComponents: String*): String =
        pathComponents match {
          case h +: tail if countArgs > 0 => pathBuilder(constantPath + "/" + h, countArgs - 1)(tail: _*)
          case Nil | _ if countArgs == 0  => constantPath
        }

      val urls: List[(Seq[String] => String, Int)] =
        List(
          ("/address/address", 2),
          ("/at", 1),
          ("/headers/at", 1),
          ("/seq", 2),
          ("/seqext", 2),
          ("/headers/seq", 2)
        ).map {
          case (p, countArgs) => (pathBuilder(p, countArgs) _, countArgs)
        }
      val pathGenerator = Gen.oneOf(urls)

      val genStr: Gen[String]      = Gen.alphaNumStr suchThat (s => s.length > 0 && Try(s.toInt).isFailure)
      val negativeInt: Gen[String] = for (i <- Arbitrary.arbitrary[Int] suchThat (_ < 0)) yield i.toString
      val nonPositiveIntStrings    = Gen.oneOf(genStr, negativeInt)

      val gen = for {
        pathFunction               <- pathGenerator
        oneNonPositiveIntString    <- nonPositiveIntStrings
        secondNonPositiveIntString <- nonPositiveIntStrings
      } yield {
        val args                  = Seq(oneNonPositiveIntString, secondNonPositiveIntString)
        val (pathFunc, countArgs) = pathFunction
        (pathFunc(args), args.take(countArgs).mkString("; "))
      }

      forAll(gen) {
        case (p, notValidPathComponents) =>
          Get(routePath(p)) ~> route ~> check {
            status shouldEqual StatusCodes.BadRequest
            responseAs[String] shouldEqual s"Path components: [$notValidPathComponents] must be positive integers"
          }
      }

    }
  }

}
