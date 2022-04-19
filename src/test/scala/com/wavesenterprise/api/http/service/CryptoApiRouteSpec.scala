package com.wavesenterprise.api.http.service

import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server.Route
import com.wavesenterprise.TestSchedulers.apiComputationsScheduler
import com.wavesenterprise.account.{Address, PublicKeyAccount}
import com.wavesenterprise.api.http.{ApiErrorResponse, ApiRoute, CryptoApiRoute, DecryptDataRequest, DecryptDataResponse, EncryptDataRequest}
import com.wavesenterprise.http.{ApiSettingsHelper, RouteSpec}
import com.wavesenterprise.protobuf.constants.CryptoAlgo
import com.wavesenterprise.settings.ApiSettings
import com.wavesenterprise.utils.Base64
import com.wavesenterprise.utils.EitherUtils.EitherExt
import com.wavesenterprise.wallet.Wallet
import com.wavesenterprise.{TestSchedulers, TestTime, TransactionGen}
import org.scalacheck.Gen
import org.scalamock.scalatest.PathMockFactory
import org.scalatest.concurrent.Eventually
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

import java.nio.charset.StandardCharsets

class CryptoApiRouteSpec
    extends RouteSpec("/crypto")
    with PathMockFactory
    with ApiSettingsHelper
    with Eventually
    with TransactionGen
    with ApiRoute
    with ScalaCheckPropertyChecks {

  protected val nodeOwner: Address = accountGen.sample.get.toAddress

  val time               = new TestTime()
  private val cryptoAlgo = CryptoAlgo.AES
  private val wallet     = mock[Wallet]
  private val service    = new CryptoApiService(wallet, TestSchedulers.cryptoServiceScheduler)
  private val cryptoRoute = new CryptoApiRoute(
    service,
    restAPISettings,
    time,
    None,
    nodeOwner,
    apiComputationsScheduler
  ).route

  override val settings: ApiSettings = restAPISettings
  override val route: Route          = cryptoRoute

  "encryption and decryption" - {
    routePath("/encryptSeparate is successful") in {
      val senderPrivKey     = accountGen.sample.get
      val recipient1PrivKey = accountGen.sample.get
      val recipient2PrivKey = accountGen.sample.get
      val password          = "some password"
      val textToEncrypt     = "it is good weather today!"
      val request = EncryptDataRequest(
        sender = senderPrivKey.address,
        password = Some(password),
        encryptionText = textToEncrypt,
        recipientsPublicKeys = List(recipient1PrivKey.publicKeyBase58, recipient2PrivKey.publicKeyBase58),
        cryptoAlgo
      )

      (wallet.privateKeyAccount _)
        .expects(senderPrivKey.toAddress, *)
        .returning(Right(senderPrivKey))
        .once()

      Post(routePath(s"/encryptSeparate"), request) ~> route ~> check {
        status shouldBe StatusCodes.OK
        val response    = responseAs[List[EncryptedSingleResponse]]
        val byPubKeyMap = response.map(encrResp => encrResp.publicKey -> encrResp).toMap

        //      ======= decrypt by first pub key =======
        (wallet.privateKeyAccount _)
          .expects(recipient1PrivKey.toAddress, *)
          .returning(Right(recipient1PrivKey))
          .once()

        val firstEncrResp = byPubKeyMap(recipient1PrivKey.publicKeyBase58)
        val firstDecryptReq = DecryptDataRequest(
          recipient = recipient1PrivKey.address,
          password = Some("doesn't matter, I will return Right(...) from wallet mock"),
          encryptedText = firstEncrResp.encryptedText,
          wrappedKey = firstEncrResp.wrappedKey,
          senderPublicKey = senderPrivKey.publicKeyBase58,
          cryptoAlgo
        )
        Post(routePath(s"/decrypt"), firstDecryptReq) ~> route ~> check {
          responseAs[DecryptDataResponse] shouldBe DecryptDataResponse(textToEncrypt)
        }

        //      ======= decrypt by second pub key =======
        (wallet.privateKeyAccount _)
          .expects(recipient2PrivKey.toAddress, *)
          .returning(Right(recipient2PrivKey))
          .once()

        val secondEncrResp = byPubKeyMap(recipient2PrivKey.publicKeyBase58)
        val secondDecryptReq = DecryptDataRequest(
          recipient = recipient2PrivKey.address,
          password = Some("doesn't matter, I will return Right(...) from wallet mock"),
          encryptedText = secondEncrResp.encryptedText,
          wrappedKey = secondEncrResp.wrappedKey,
          senderPublicKey = senderPrivKey.publicKeyBase58,
          cryptoAlgo
        )
        Post(routePath(s"/decrypt"), secondDecryptReq) ~> route ~> check {
          responseAs[DecryptDataResponse] shouldBe DecryptDataResponse(textToEncrypt)
        }
      }
    }
    routePath("/encryptCommon is successful") in {
      val senderPrivateKey   = accountGen.sample.get
      val recipientsAccounts = Gen.listOfN(10, accountGen).sample.get
      val recipientPubkeyToPrivate = recipientsAccounts.map { privateAccount =>
        privateAccount.toAddress -> privateAccount
      }.toMap
      val password      = "some password"
      val textToEncrypt = "it is good weather today!"

      val request = EncryptDataRequest(
        sender = senderPrivateKey.address,
        password = Some(password),
        encryptionText = textToEncrypt,
        recipientsPublicKeys = recipientsAccounts.map(_.publicKeyBase58),
        cryptoAlgo
      )

      (wallet.privateKeyAccount _)
        .expects(senderPrivateKey.toAddress, *)
        .returning(Right(senderPrivateKey))
        .once()

      Post(routePath(s"/encryptCommon"), request) ~> route ~> check {
        status shouldBe StatusCodes.OK
        val response = responseAs[EncryptedForManyResponse]

        //      ======= decrypt each =======
        response.recipientToWrappedStructure.foreach {
          case (pubKeyBase58, wrappedStructureBase58) =>
            val publicKeyFromResponse = PublicKeyAccount.fromBase58String(pubKeyBase58).explicitGet()
            val recipientPrivateKey   = recipientPubkeyToPrivate(publicKeyFromResponse.toAddress)

            (wallet.privateKeyAccount _)
              .expects(publicKeyFromResponse.toAddress, *)
              .returning(Right(recipientPrivateKey))
              .once()

            val secondDecryptReq = DecryptDataRequest(
              recipient = recipientPrivateKey.address,
              password = Some("doesn't matter, I will return Right(...) from wallet mock"),
              encryptedText = response.encryptedText,
              wrappedKey = wrappedStructureBase58,
              senderPublicKey = senderPrivateKey.publicKeyBase58,
              cryptoAlgo
            )
            Post(routePath(s"/decrypt"), secondDecryptReq) ~> route ~> check {
              responseAs[DecryptDataResponse] shouldBe DecryptDataResponse(textToEncrypt)
            }
        }
      }
    }
  }

  "decryption fail cases" - {
    val senderPrivKey    = accountGen.sample.get
    val recipientPrivKey = accountGen.sample.get
    val password         = "very strong password"
    val textToEncrypt    = "I do not care about this text"
    val request = EncryptDataRequest(
      sender = senderPrivKey.address,
      password = Some(password),
      encryptionText = textToEncrypt,
      recipientsPublicKeys = List(recipientPrivKey.publicKeyBase58),
      cryptoAlgo
    )

    (wallet.privateKeyAccount _)
      .expects(senderPrivKey.toAddress, *)
      .returning(Right(senderPrivKey))
      .once()

    Post(routePath(s"/encryptSeparate"), request) ~> route ~> check {
      status shouldBe StatusCodes.OK
      val response = responseAs[List[EncryptedSingleResponse]].head

      routePath("/decrypt return none if wrong encryption text") in {
        (wallet.privateKeyAccount _)
          .expects(recipientPrivKey.toAddress, *)
          .returning(Right(recipientPrivKey))
          .once()

        val alteredEncryptedText = Base64
          .decode(response.encryptedText)
          .map(originalTextBytes => originalTextBytes ++ "oops".getBytes(StandardCharsets.UTF_8))
          .map(Base64.encode)
          .get

        val firstDecryptReq = DecryptDataRequest(
          recipient = recipientPrivKey.address,
          password = Some("doesn't matter, I will return Right(...) from wallet mock"),
          encryptedText = alteredEncryptedText,
          wrappedKey = response.wrappedKey,
          senderPublicKey = senderPrivKey.publicKeyBase58,
          cryptoAlgo
        )
        Post(routePath(s"/decrypt"), firstDecryptReq) ~> route ~> check {
          val response = responseAs[ApiErrorResponse]
          response.error shouldBe 199
          response.message should include("Decryption failed")
        }
      }

      routePath("/decrypt return none if wrong wrapped key") in {
        (wallet.privateKeyAccount _)
          .expects(recipientPrivKey.toAddress, *)
          .returning(Right(recipientPrivKey))
          .once()

        val firstDecryptReq = DecryptDataRequest(
          recipient = recipientPrivKey.address,
          password = Some("doesn't matter, I will return Right(...) from wallet mock"),
          encryptedText = response.encryptedText,
          wrappedKey = response.wrappedKey + "oops",
          senderPublicKey = senderPrivKey.publicKeyBase58,
          cryptoAlgo
        )
        Post(routePath(s"/decrypt"), firstDecryptReq) ~> route ~> check {
          val response = responseAs[ApiErrorResponse]
          response.error shouldBe 199
          response.message should include("Decryption failed")
        }
      }
    }
  }

}
