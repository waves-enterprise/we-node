package com.wavesenterprise.privacy.db

import com.wavesenterprise.privacy.PolicyMetaData
import slick.jdbc.JdbcProfile

class PolicyMetaDataDBIO(protected val driver: JdbcProfile) extends CommonSlickTrait {

  import driver.api._

  class PolicyMetaDataTable(tag: Tag) extends Table[PolicyMetaData](tag, PolicyMetaDataTable.TableName) {

    def policyId: Rep[String]   = column[String](PolicyMetaDataTable.PolicyId)
    def hash: Rep[String]       = column[String](PolicyMetaDataTable.Hash, O.PrimaryKey)
    def sender: Rep[String]     = column[String]("sender")
    def filename: Rep[String]   = column[String]("filename")
    def size: Rep[Int]          = column[Int]("size")
    def timestamp: Rep[Long]    = column[Long]("timestamp")
    def author: Rep[String]     = column[String]("author")
    def comment: Rep[String]    = column[String]("comment")
    def loId: Rep[Option[Long]] = column[Option[Long]]("lo_id")

    def * =
      (policyId, hash, sender, filename, size, timestamp, author, comment, loId) <> (PolicyMetaData.tupled, PolicyMetaData.postgresExtractor().lift)
  }

  object PolicyMetaDataTable {
    val TableName = "policy_meta_data"
    val PolicyId  = "policy_id"
    val Hash      = "hash"
  }

  lazy val policyMetaDataTable = TableQuery[PolicyMetaDataTable]

  def all(): DBIO[Seq[PolicyMetaData]] =
    policyMetaDataTable.result

  def insert(policyMetaData: PolicyMetaData): DBIO[Int] =
    policyMetaDataTable += policyMetaData

  def deleteAll(): DBIO[Int] =
    policyMetaDataTable.delete

  def delete(policyId: String, hash: String): DBIO[Int] =
    policyMetaDataTable
      .filter(entry => entry.policyId === policyId && entry.hash === hash)
      .delete

  def policyHashes(policyId: String): DBIO[Seq[String]] =
    policyMetaDataTable
      .filter(_.policyId === policyId)
      .map(_.hash)
      .result

  def isLargeObject(policyId: String, hash: String): DBIO[Option[Boolean]] =
    policyMetaDataTable
      .filter { meta =>
        meta.policyId === policyId &&
        meta.hash === hash
      }
      .map(_.loId.isDefined)
      .result
      .headOption

  def policyItemMeta(policyId: String, hash: String): DBIO[Option[PolicyMetaData]] =
    policyMetaDataTable
      .filter { meta =>
        meta.policyId === policyId &&
        meta.hash === hash
      }
      .result
      .headOption

  def policyItemsMetas(policyIds: Set[String], hashes: Set[String]): DBIO[Seq[PolicyMetaData]] =
    policyMetaDataTable.filter { meta =>
      (meta.policyId inSet policyIds) &&
      (meta.hash inSet hashes)
    }.result

  def exists(policyId: String, hash: String): DBIO[Boolean] =
    sql"""SELECT exists(SELECT 1 FROM #${PolicyMetaDataTable.TableName} WHERE (#${PolicyMetaDataTable.PolicyId} = $policyId) AND (#${PolicyMetaDataTable.Hash} = $hash))"""
      .as[Boolean]
      .head
}
