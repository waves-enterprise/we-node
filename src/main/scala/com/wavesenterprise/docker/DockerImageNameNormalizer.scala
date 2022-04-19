package com.wavesenterprise.docker

import cats.implicits._
import org.apache.commons.lang3.StringUtils

object DockerImageNameNormalizer {

  val DigestPrefix = "@sha256:"
  val LatestTag    = "latest"

  def normalize(image: String, defaultRegistryDomain: Option[String]): Either[ContractExecutionException, DockerImageName] =
    Either
      .catchNonFatal {
        val (domain, reference) = {
          val i = image.indexOf('/')
          if (i == -1) {
            (defaultRegistryDomain, image)
          } else {
            val (domain, reference) = (image.take(i), image.drop(i + 1))
            if (StringUtils.containsAny(domain, '.', ':') || domain == "localhost") {
              (Some(domain), reference)
            } else {
              (defaultRegistryDomain, image)
            }
          }
        }
        val (component, tag, digest) = {
          val digestIndex = reference.indexOf(DigestPrefix)
          if (digestIndex == -1) {
            val tagIndex = reference.indexOf(':')
            if (tagIndex == -1) {
              (reference, Some(LatestTag), None)
            } else {
              val (component, tag) = (reference.take(tagIndex), reference.drop(tagIndex + 1))
              (component, Some(tag), None)
            }
          } else {
            val (component, digest) = (reference.take(digestIndex), reference.drop(digestIndex + DigestPrefix.length))
            (component, None, Some(digest))
          }
        }
        DockerImageName(image, domain, component, tag, digest)
      }
      .leftMap(e => new ContractExecutionException(s"Can't normalize contract image name '$image'", e))
}

case class DockerImageName(image: String, domain: Option[String], component: String, tag: Option[String], digest: Option[String], fullName: String)

object DockerImageName {

  import DockerImageNameNormalizer._

  def apply(image: String, domain: Option[String], component: String, tag: Option[String], digest: Option[String]): DockerImageName = {
    val fullName = domain.map(_ + "/").getOrElse("") + component + tag.map(":" + _).getOrElse("") + digest.map(DigestPrefix + _).getOrElse("")
    DockerImageName(image, domain, component, tag, digest, fullName)
  }
}
