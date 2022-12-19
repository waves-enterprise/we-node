package com.wavesenterprise.http

import java.net.{InetAddress, InetSocketAddress}

import com.wavesenterprise.TestSchedulers.apiComputationsScheduler
import akka.http.scaladsl.model.StatusCodes
import com.wavesenterprise.account.Address
import com.wavesenterprise.api.http.ApiError.ApiKeyNotValid
import com.wavesenterprise.api.http.service.PeersApiService
import com.wavesenterprise.api.http.service.PeersApiService.NetworkInfo
import com.wavesenterprise.api.http.{AddressWithPublicKey, AllowNodesResponse, PeersApiRoute}
import com.wavesenterprise.http.ApiMarshallers._
import com.wavesenterprise.network.peers.{ActivePeerConnections, PeerConnection, PeerDatabase, PeerInfo}
import com.wavesenterprise.state.Blockchain
import com.wavesenterprise.{NoShrink, NodeVersion, TestTime, TransactionGen}
import io.netty.channel.Channel
import io.netty.channel.local.LocalChannel
import org.scalacheck.{Arbitrary, Gen}
import org.scalamock.matchers.Matcher
import org.scalamock.scalatest.MockFactory
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks
import play.api.libs.json.{Format, JsObject, JsValue, Json}

class PeersRouteSpec
    extends RouteSpec("/peers")
    with ApiSettingsHelper
    with ScalaCheckPropertyChecks
    with MockFactory
    with TransactionGen
    with NoShrink {

  import PeersRouteSpec._

  private val ownerAddress: Address = accountGen.sample.get.toAddress

  private val peerDatabase     = mock[PeerDatabase]
  private val mockedBlockchain = mock[Blockchain]
  private val connectToPeer    = mockFunction[InetSocketAddress, Unit]
  private val inetAddressGen   = Gen.listOfN(4, Arbitrary.arbitrary[Byte]).map(_.toArray).map(InetAddress.getByAddress)
  private val inetSocketAddressGen = for {
    address <- inetAddressGen
    port    <- Gen.chooseNum(0, 0xffff)
  } yield new InetSocketAddress(address, port)

  private val time = new TestTime

  private val versionGen = for {
    major <- Gen.chooseNum(0, 3)
    minor <- Gen.chooseNum(0, 3)
    patch <- Gen.chooseNum(0, 3)
  } yield NodeVersion(major, minor, patch)

  private def genListOf[A](maxLength: Int, src: Gen[A]) = Gen.chooseNum(0, maxLength).flatMap(n => Gen.listOfN(n, src))

  routePath("/connected") in {
    val gen =
      for {
        remoteAddress        <- inetSocketAddressGen
        declaredAddress      <- Gen.option(inetSocketAddressGen)
        nodeName             <- Gen.alphaNumStr
        nodeNonce            <- Arbitrary.arbitrary[Int]
        chainId              <- Gen.alphaChar
        nodeVersion          <- versionGen
        applicationConsensus <- Gen.alphaNumStr
        nodeOwnerAddress     <- addressGen
        sessionKey           <- accountGen
      } yield new PeerConnection(
        new LocalChannel(),
        PeerInfo(remoteAddress, declaredAddress, chainId, nodeVersion, applicationConsensus, nodeName, nodeNonce, nodeOwnerAddress, sessionKey),
        sessionKey
      )

    forAll(genListOf(TestsCount, gen)) { l: List[PeerConnection] =>
      val connections = new ActivePeerConnections(100)
      val service     = new PeersApiService(mockedBlockchain, peerDatabase, connectToPeer, connections, time)
      val route       = new PeersApiRoute(service, restAPISettings, time, ownerAddress, apiComputationsScheduler).route
      l.foreach(connections.putIfAbsentAndMaxNotReachedOrReplaceValidator(_))

      Get(routePath("/connected")) ~> route ~> check {
        responseAs[Connected].peers should contain theSameElementsAs l.map { pc =>
          val pi = pc.peerInfo
          ConnectedPeer(
            pi.remoteAddress.toString,
            pi.declaredAddress.fold("N/A")(_.toString),
            pi.nodeOwnerAddress.stringRepr,
            pi.nodeName,
            pi.nodeNonce,
            pi.nodeVersion.asFlatString,
            pi.applicationConsensus
          )
        }
      }
    }
  }

  routePath("/all") in {
    val gen = for {
      inetAddress <- inetSocketAddressGen
      ts          <- Gen.posNum[Long]
    } yield inetAddress -> ts

    forAll(genListOf(TestsCount, gen)) { m =>
      (peerDatabase.knownPeers _).expects().returning(m.toMap[InetSocketAddress, Long])
      val service =
        new PeersApiService(mockedBlockchain, peerDatabase, connectToPeer, new ActivePeerConnections(100), time)
      val route = new PeersApiRoute(service, restAPISettings, time, ownerAddress, apiComputationsScheduler).route

      Get(routePath("/all")) ~> route ~> check {
        responseAs[AllPeers].peers should contain theSameElementsAs m.map {
          case (address, timestamp) => Peer(address.toString, timestamp)
        }
      }
    }
  }

  routePath("/connect") in {
    val service =
      new PeersApiService(mockedBlockchain, peerDatabase, connectToPeer, new ActivePeerConnections(100), time)
    val route      = new PeersApiRoute(service, restAPISettings, time, ownerAddress, apiComputationsScheduler).route
    val connectUri = routePath("/connect")
    Post(connectUri, ConnectReq("example.com", 1)) ~> route should produce(ApiKeyNotValid)
    Post(connectUri, "") ~> api_key(apiKey) ~> route ~> check(handled shouldEqual false)
    Post(connectUri, Json.obj()) ~> api_key(apiKey) ~> route ~> check {
      (responseAs[JsValue] \ "validationErrors").as[JsObject].keys should not be 'empty
    }

    val address = inetSocketAddressGen.sample.get
    connectToPeer.expects(address).once
    Post(connectUri, ConnectReq(address.getHostName, address.getPort)) ~> api_key(apiKey) ~> route ~> check {
      responseAs[ConnectResp].hostname shouldEqual address.getHostName
    }
  }

  routePath("/allowedNodes with managedPrivacySettings") in {
    val tenPrivateKeys = Gen.listOfN(10, accountGen).sample.get.map(pk => pk.toAddress -> pk).toMap
    val expectedResponse = AllowNodesResponse(
      allowedNodes = tenPrivateKeys.map {
        case (addr, pk) =>
          AddressWithPublicKey(addr.stringRepr, pk.publicKeyBase58)
      }.toSeq,
      time.correctedTime()
    )

    val blockchain = mock[Blockchain]
    (blockchain.networkParticipants _).expects().returning(tenPrivateKeys.keys.toSeq)
    tenPrivateKeys.foreach {
      case (addr, pk) =>
        (blockchain.participantPubKey _).expects(addr).returning(Some(pk))
    }
    val service =
      new PeersApiService(
        blockchain,
        peerDatabase,
        connectToPeer,
        new ActivePeerConnections(100),
        time
      )
    val route      = new PeersApiRoute(service, restAPISettings, time, ownerAddress, apiComputationsScheduler).route
    val connectUri = routePath("/allowedNodes")
    Get(connectUri) ~> route should produce(ApiKeyNotValid)
    Get(connectUri) ~> api_key(apiKey) ~> route ~> check {
      status shouldEqual StatusCodes.OK
      responseAs[AllowNodesResponse] shouldBe expectedResponse
    }
  }

  routePath("/hostname/{address} check not connected address") in {
    val address                    = addressGen.sample.get
    val activePeersConnectionsMock = mock[ActivePeerConnections]
    val service =
      new PeersApiService(
        mockedBlockchain,
        peerDatabase,
        connectToPeer,
        activePeersConnectionsMock,
        time
      )

    val addressMatcher = new Matcher[Address] {
      override def safeEquals(that: Address): Boolean = {
        that match {
          case argumentAddress: Address =>
            address.stringRepr == argumentAddress.stringRepr
          case _ => false
        }
      }
    }

    (activePeersConnectionsMock.channelForAddress _).expects(addressMatcher).returning(None).once()

    val route = new PeersApiRoute(service, restAPISettings, time, ownerAddress, apiComputationsScheduler).route
    Get(routePath(s"/hostname/${address.address}")) ~> api_key(apiKey) ~> route ~> runRoute ~> check {
      responseAs[NetworkInfo] shouldBe NetworkInfo(connected = false, None, None)
    }
  }

  routePath("/hostname/{address} check connected address") in {
    val address                    = addressGen.sample.get
    val activePeersConnectionsMock = mock[ActivePeerConnections]
    val service =
      new PeersApiService(
        mockedBlockchain,
        peerDatabase,
        connectToPeer,
        activePeersConnectionsMock,
        time
      )

    val addressMatcher = new Matcher[Address] {
      override def safeEquals(that: Address): Boolean = {
        that match {
          case argumentAddress: Address =>
            address.stringRepr == argumentAddress.stringRepr
          case _ => false
        }
      }
    }

    val hostname    = "localhost"
    val channelMock = mock[Channel]
    (() => channelMock.remoteAddress()).expects().returning(new InetSocketAddress(hostname, 22))
    (activePeersConnectionsMock.channelForAddress _).expects(addressMatcher).returning(Some(channelMock)).once()

    val route = new PeersApiRoute(service, restAPISettings, time, ownerAddress, apiComputationsScheduler).route
    Get(routePath(s"/hostname/${address.address}")) ~> api_key(apiKey) ~> route ~> runRoute ~> check {
      val response = responseAs[NetworkInfo]
      response shouldBe NetworkInfo(connected = true, Some(hostname), Some("127.0.0.1"))
    }
  }

}

object PeersRouteSpec {
  val TestsCount = 20

  case class ConnectReq(host: String, port: Int)

  implicit val connectReqFormat: Format[ConnectReq] = Json.format

  case class ConnectResp(status: String, hostname: String)

  implicit val connectRespFormat: Format[ConnectResp] = Json.format

  case class ConnectedPeer(address: String,
                           declaredAddress: String,
                           nodeOwnerAddress: String,
                           peerName: String,
                           peerNonce: Long,
                           applicationVersion: String,
                           applicationConsensus: String)

  implicit val connectedPeerFormat: Format[ConnectedPeer] = Json.format

  case class Connected(peers: Seq[ConnectedPeer])

  implicit val connectedFormat: Format[Connected] = Json.format

  case class Peer(address: String, lastSeen: Long)

  implicit val peerFormat: Format[Peer] = Json.format

  case class AllPeers(peers: Seq[Peer])

  implicit val allPeersFormat: Format[AllPeers] = Json.format
}
