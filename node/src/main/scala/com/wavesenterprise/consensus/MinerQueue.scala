package com.wavesenterprise.consensus

import cats.kernel.Monoid
import com.wavesenterprise.account.Address
import com.wavesenterprise.acl.{PermissionOp, Permissions, Role}

import scala.collection.immutable.SortedSet

sealed trait MinerQueue {

  /** All addresses that have ever been miners. May include currently inactive miners. */
  def suggestedMiners: Seq[Address]
  def minerPermissions: Map[Address, Permissions]
  def currentMinersSet(timestamp: Long): SortedSet[Address]
  def update(address: Address, newPermissions: Permissions): MinerQueue
  def update(newPermissions: Map[Address, Permissions]): MinerQueue
  def remove(address: Address, permissionOp: PermissionOp): MinerQueue
}

object MinerQueue {
  implicit val monoidalInstance: Monoid[MinerQueue] = new Monoid[MinerQueue] {
    override def empty: MinerQueue = MinerQueue(Map.empty[Address, Permissions])

    override def combine(x: MinerQueue, y: MinerQueue): MinerQueue = {
      val xPermissions = x.minerPermissions
      val resultPermissions = x.minerPermissions ++ y.minerPermissions.map {
        case (address, permissions) =>
          address -> permissions.combine(xPermissions.getOrElse(address, Permissions.empty))
      }
      MinerQueue(resultPermissions)
    }
  }

  def empty: MinerQueue = monoidalInstance.empty

  private def filterMiners(permissions: Permissions): Permissions = {
    permissions.filter(op => op.role == Role.Miner)
  }

  def apply(permissions: Map[Address, Permissions]): MinerQueue = {
    val minerPermissions = for {
      (address, per) <- permissions
      filtered = filterMiners(per)
      if !filtered.isEmpty
    } yield address -> filtered

    MinerQueueImpl(minerPermissions)
  }

  private case class MinerQueueImpl(minerPermissions: Map[Address, Permissions]) extends MinerQueue { self =>
    def suggestedMiners: Seq[Address] = minerPermissions.keys.toSeq

    def currentMinersSet(timestamp: Long): SortedSet[Address] = {
      val relation = for {
        (address, permissions) <- minerPermissions
        lastMinerPermissionOp  <- permissions.lastActive(Role.Miner, timestamp)
      } yield (address, lastMinerPermissionOp)
      implicit val permissionOrd: Ordering[PermissionOp] = Permissions.ascendingOrdering
      implicit val addressOrd: Ordering[Address]         = Ordering.by(relation.get)

      relation.keySet.to[SortedSet]
    }

    override def update(address: Address, newPermissions: Permissions): MinerQueue = {
      val newMinerPermissionOps = newPermissions.toSeq.filter(_.role == Role.Miner)

      if (newMinerPermissionOps.nonEmpty) {
        val newMinerPermissions = Permissions(newMinerPermissionOps)
        val updatedPermissions = minerPermissions.get(address) match {
          case Some(found) => minerPermissions.updated(address, found.combine(newMinerPermissions))
          case None        => minerPermissions + (address -> newMinerPermissions)
        }
        MinerQueue(updatedPermissions)
      } else {
        self
      }
    }

    override def update(newPermissions: Map[Address, Permissions]): MinerQueue = {
      newPermissions.foldLeft(self: MinerQueue) {
        case (miners, (address, permissions)) =>
          miners.update(address, permissions)
      }
    }

    def remove(address: Address, permissionOp: PermissionOp): MinerQueue = {
      if (permissionOp.role == Role.Miner) {
        minerPermissions.get(address) match {
          case Some(permissions) =>
            MinerQueue(minerPermissions.updated(address, Permissions(permissions.toSeq.filterNot(_ == permissionOp))))
          case None => self
        }
      } else {
        self
      }
    }
  }
}
