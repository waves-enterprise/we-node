package com.wavesenterprise.utils.pki

import cats.Monoid

import java.security.cert.X509CRL

case class CrlCollection(newCrlData: Set[CrlData], crls: Set[X509CRL])

object CrlCollection {
  val empty: CrlCollection = CrlCollection(Set.empty, Set.empty)

  def apply(crlData: CrlData): CrlCollection   = CrlCollection.apply(newCrlData = Set(crlData), crls = Set.empty)
  def apply(crls: Set[X509CRL]): CrlCollection = CrlCollection.apply(newCrlData = Set.empty, crls = crls)

  implicit val crlCollectionMonoid: Monoid[CrlCollection] = new Monoid[CrlCollection] {
    override def empty: CrlCollection = CrlCollection.empty

    override def combine(x: CrlCollection, y: CrlCollection): CrlCollection = {
      CrlCollection(
        newCrlData = x.newCrlData ++ y.newCrlData,
        crls = x.crls ++ y.crls
      )
    }
  }
}
