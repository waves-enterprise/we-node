package com.wavesenterprise.http

import com.wavesenterprise.TestSchedulers.apiComputationsScheduler
import akka.http.scaladsl.model.StatusCodes
import com.wavesenterprise.account.Address
import com.wavesenterprise.acl.{OpType, PermissionOp, Permissions, Role}
import com.wavesenterprise.acl.PermissionsGen._
import com.wavesenterprise.api.http.acl.{PermissionApiRoute, PermissionsForAddressesReq}
import com.wavesenterprise.api.http.service.PermissionApiService
import com.wavesenterprise.api.http.service.PermissionApiService.{RoleInfo, RolesForAddressResponse}
import com.wavesenterprise.http.ApiMarshallers._
import com.wavesenterprise.state.Blockchain
import com.wavesenterprise.utils.EitherUtils.EitherExt
import com.wavesenterprise.utx.UtxPool
import com.wavesenterprise.{NoShrink, TestTime, TestWallet, TransactionGen}
import org.scalacheck.Gen
import com.wavesenterprise.BlockGen
import com.wavesenterprise.consensus.ContractValidatorPool
import org.scalamock.scalatest.PathMockFactory
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks
import play.api.libs.json.{JsObject, Json}

class PermissionRouteSpec
    extends RouteSpec("/permissions")
    with PathMockFactory
    with ScalaCheckPropertyChecks
    with ApiSettingsHelper
    with TransactionGen
    with TestWallet
    with NoShrink
    with BlockGen {

  private val ownerAddress: Address = accountGen.sample.get.toAddress

  private val blockchain = stub[Blockchain]
  private val utxPool    = mock[UtxPool]

  private val route =
    new PermissionApiRoute(restAPISettings, utxPool, new TestTime, new PermissionApiService(blockchain), ownerAddress, apiComputationsScheduler).route

  routePath("/{address}") in {
    forAll(permissionsGen, accountGen) { (perms, account) =>
      val address = Address.fromPublicKey(account.publicKey)
      (blockchain.permissions _).when(address).returns(perms)

      Get(routePath(s"/${address.stringRepr}")) ~> route ~> check {
        handled shouldBe true
        status shouldBe StatusCodes.OK
        val json = responseAs[JsObject]
        (json \ "roles").isDefined shouldBe true
        (json \ "timestamp").isDefined shouldBe true
      }
    }
  }

  routePath("/{address}/at/{timestamp}") in {
    forAll(permissionsGen, accountGen, timestampGen) { (perms, account, timestamp) =>
      val address = Address.fromPublicKey(account.publicKey)
      (blockchain.permissions _).when(address).returns(perms)

      Get(routePath(s"/${address.stringRepr}/at/$timestamp")) ~> route ~> check {
        handled shouldBe true
        status shouldBe StatusCodes.OK
        val activePermissions = perms.active(timestamp)
        val json              = responseAs[JsObject]
        (json \ "roles").as[Seq[RoleInfo]].map(_.role) should contain allElementsOf activePermissions.toSeq.map(_.prefixS)
        (json \ "timestamp").as[Long] shouldEqual timestamp
      }
    }
  }

  routePath("/contractValidators") in {
    var contractPool = Map.empty[Address, Permissions]
    val perm         = Permissions(Seq(PermissionOp(OpType.Add, Role.ContractValidator, 10000000, None)))
    forAll(accountGen) { (account) =>
      val address = Address.fromPublicKey(account.publicKey)
      contractPool += (address -> perm)
    }

    forAll(randomSignerBlockGen) { block =>
      (blockchain.lastBlock _)
        .when()
        .returning(Some(block.block))
      (blockchain.contractValidators _).when().returning(ContractValidatorPool(contractPool))

      Get(routePath(s"/contractValidators")) ~> route ~> check {
        handled shouldBe true
        status shouldBe StatusCodes.OK
        val json = responseAs[JsObject]
        println(json)
        (json \ "addresses").isEmpty shouldBe false
      }
    }

  }

  val genAddressesAndPermissions: Gen[Map[Address, Permissions]] =
    for {
      n           <- Gen.chooseNum(1, 20)
      addresses   <- Gen.listOfN(n, accountGen.map(acc => Address.fromPublicKey(acc.publicKey)))
      permissions <- Gen.listOfN(n, permissionsGen)
    } yield addresses.zip(permissions).toMap

  routePath("/addresses") in {
    forAll(genAddressesAndPermissions, timestampGen) { (addressesToPermissions, timestamp) =>
      addressesToPermissions.foreach {
        case (address, perms) =>
          (blockchain.permissions _).when(address).returns(perms)
      }
      val addresses = addressesToPermissions.keys.toSeq

      Post(routePath("/addresses"), Json.toJson(PermissionsForAddressesReq(addresses.map(_.address), timestamp))) ~> route ~> check {
        handled shouldBe true
        status shouldBe StatusCodes.OK
        val json = responseAs[JsObject]
        (json \ "timestamp").as[Long] shouldEqual timestamp
        val rolesSeq = (json \ "addressToRoles").as[Seq[RolesForAddressResponse]]
        rolesSeq.foreach { roleForAddr =>
          val responseAddress = Address.fromString(roleForAddr.address).explicitGet()
          val sourcePerms     = addressesToPermissions(responseAddress)
          val sourceRoleInfos = sourcePerms.activeAsOps(timestamp).map(RoleInfo.fromPermissionOp)
          roleForAddr.roles should contain theSameElementsAs sourceRoleInfos
        }
      }
    }
  }

}
