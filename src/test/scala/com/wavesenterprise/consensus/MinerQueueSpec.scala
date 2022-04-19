package com.wavesenterprise.consensus

import com.wavesenterprise.{NoShrink, TransactionGen}
import com.wavesenterprise.account.Address
import com.wavesenterprise.acl.PermissionsGen._
import com.wavesenterprise.acl.{Permissions, PermissionsGen, Role}
import com.wavesenterprise.wallet.Wallet
import org.scalacheck.Gen
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks
import org.scalatest.{FreeSpec, Matchers}

class MinerQueueSpec extends FreeSpec with Matchers with ScalaCheckPropertyChecks with TransactionGen with NoShrink {

  val permissionsWithAddress: Gen[Map[Address, Permissions]] =
    Gen.mapOf(for {
      a <- addressGen
      p <- minerPermissionsGen
    } yield (a, p))

  val permissionsWithAddressIds: Gen[(BigInt, Permissions)] =
    for {
      addressId <- Gen.chooseNum(0, Long.MaxValue)
      perms     <- minerPermissionsGen
    } yield BigInt(addressId) -> perms

  "only miners permissions ops Miners" in {
    forAll(permissionsGen, addressGen) {
      case (permissions, address) =>
        MinerQueue(Map(address -> permissions)).minerPermissions
          .forall(_._2.toSeq.forall(op => op.role == Role.Miner)) shouldEqual true
    }
  }

  "order test" in {
    forAll(permissionsWithAddress, timestampGen) {
      case (permissions, ts) =>
        val highestTs: Long = {
          val timestamps = permissions.values.flatMap(_.toSeq.map(_.timestamp))
          if (timestamps.nonEmpty) timestamps.max
          else ts
        }

        val addresses = permissions.toSeq
          .filter(_._2.contains(Role.Miner, highestTs))
          .sortWith {
            case ((_, x), (_, y)) =>
              x.toSeq.head.timestamp < y.toSeq.head.timestamp
          }
          .map(_._1)

        MinerQueue(permissions).currentMinersSet(highestTs).toSeq shouldEqual addresses
    }
  }

  "Is correctly constructed from Permissions" - {
    "Queue contains addresses with Miner roles (without dueTimestamps)" in {
      forAll(Gen.listOf(PermissionsGen.permissionOpAddWithoutDueGen)) { permOps =>
        val addressToPermissions: Map[Address, Permissions] = permOps.map { permOp =>
          Wallet.generateNewAccount().toAddress -> Permissions(Seq(permOp))
        }.toMap

        val expectedMiners: Seq[Address] = addressToPermissions.collect {
          case (address, permissions) if permissions.contains(Role.Miner, onTimestamp = Long.MaxValue) =>
            address
        }.toSeq

        val minerQueue = MinerQueue(addressToPermissions)

        minerQueue.suggestedMiners should contain theSameElementsAs expectedMiners
      }
    }

    "Queue contains addresses with Miner roles (with dueTimestamps)" in {
      forAll(Gen.listOf(PermissionsGen.permissionOpAddGen), timestampGen) { (permOps, referenceTimestamp) =>
        val addressToPermissions: Map[Address, Permissions] = permOps.map { permOp =>
          Wallet.generateNewAccount().toAddress -> Permissions(Seq(permOp))
        }.toMap

        val expectedMiners: Seq[Address] = addressToPermissions.collect {
          case (address, permissions) if permissions.contains(Role.Miner, referenceTimestamp) =>
            address
        }.toSeq

        val minerQueue = MinerQueue(addressToPermissions)

        minerQueue.currentMinersSet(referenceTimestamp).toSeq should contain theSameElementsAs expectedMiners
      }
    }

    "Queue doesn't contain addresses with expired Miner roles" in {
      val timestampsAndPermOpsGen = for {
        timestamps <- Gen
          .listOfN(5, narrowTimestampGen)
          .retryUntil(timestamps => timestamps.distinct.length == timestamps.length)
        List(issuedBefore, validTimestamp, dueAfter, dueBefore, invalidTimestamp) = timestamps.sorted
        permOps <- Gen.listOf(PermissionsGen.permissionOpGen(issuedBefore, dueAfter, dueBefore))
      } yield (invalidTimestamp, validTimestamp, permOps)

      forAll(timestampsAndPermOpsGen) {
        case (invalidTs, validTs, permOps) =>
          val addressToPermissions = permOps.map { permOp =>
            Wallet.generateNewAccount().toAddress -> Permissions(Seq(permOp))
          }.toMap

          val expectedMiners: Seq[Address] = addressToPermissions.collect {
            case (address, permissions) if permissions.contains(Role.Miner, validTs) =>
              address
          }.toSeq

          val minerQueue = MinerQueue(addressToPermissions)

          minerQueue.currentMinersSet(invalidTs) shouldBe 'empty
          minerQueue.currentMinersSet(validTs).toSeq should contain theSameElementsAs expectedMiners
      }
    }
  }
}
