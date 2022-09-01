package com.wavesenterprise.docker

import org.scalamock.scalatest.MockFactory
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers

class NormalizeImageNameTestSuite extends AnyFreeSpec with Matchers with MockFactory with ScalaCheckPropertyChecks {

  import NormalizeImageNameTestSuite._

  "NormalizeImageNameTestSuite" - {
    "test normalize image name with tag" in {
      val image = "localhost:5000/stateful-increment-contract:v1.0"
      val expected = DockerImageName(image,
                                     Some("localhost:5000"),
                                     "stateful-increment-contract",
                                     Some("v1.0"),
                                     None,
                                     "localhost:5000/stateful-increment-contract:v1.0")
      DockerImageNameNormalizer.normalize(image, Some(DefaultRegistryDomain)) shouldBe Right(expected)
    }

    "test normalize image name with 'latest' tag" in {
      val image = "localhost:5000/stateful-increment-contract:latest"
      val expected = DockerImageName(image,
                                     Some("localhost:5000"),
                                     "stateful-increment-contract",
                                     Some("latest"),
                                     None,
                                     "localhost:5000/stateful-increment-contract:latest")
      DockerImageNameNormalizer.normalize(image, Some(DefaultRegistryDomain)) shouldBe Right(expected)
    }

    "test normalize image name without tag" in {
      val image = "localhost:5000/stateful-increment-contract"
      val expected = DockerImageName(image,
                                     Some("localhost:5000"),
                                     "stateful-increment-contract",
                                     Some("latest"),
                                     None,
                                     "localhost:5000/stateful-increment-contract:latest")
      DockerImageNameNormalizer.normalize(image, Some(DefaultRegistryDomain)) shouldBe Right(expected)
    }

    "test normalize image name without registry and tag" in {
      val image = "stateful-increment-contract"
      val expected = DockerImageName(image,
                                     Some(DefaultRegistryDomain),
                                     "stateful-increment-contract",
                                     Some("latest"),
                                     None,
                                     s"$DefaultRegistryDomain/stateful-increment-contract:latest")
      DockerImageNameNormalizer.normalize(image, Some(DefaultRegistryDomain)) shouldBe Right(expected)
    }

    "test normalize image name with digest" in {
      val image = "localhost:5000/stateful-increment-contract@sha256:7113654181992977d96f4fc0561b3a3a749e2d6a8c8129cf5ce13ccf94effd2b"
      val expected =
        DockerImageName(
          image,
          Some("localhost:5000"),
          "stateful-increment-contract",
          None,
          Some("7113654181992977d96f4fc0561b3a3a749e2d6a8c8129cf5ce13ccf94effd2b"),
          "localhost:5000/stateful-increment-contract@sha256:7113654181992977d96f4fc0561b3a3a749e2d6a8c8129cf5ce13ccf94effd2b"
        )
      DockerImageNameNormalizer.normalize(image, Some(DefaultRegistryDomain)) shouldBe Right(expected)
    }

    "test normalize name with repository" in {
      val image = "localhost:5000/we-sc/stateful-increment-contract:v1.0"
      val expected = DockerImageName(image,
                                     Some("localhost:5000"),
                                     "we-sc/stateful-increment-contract",
                                     Some("v1.0"),
                                     None,
                                     "localhost:5000/we-sc/stateful-increment-contract:v1.0")
      DockerImageNameNormalizer.normalize(image, Some(DefaultRegistryDomain)) shouldBe Right(expected)
    }

    "test normalize name with repository and without registry" in {
      val image = "we-sc/stateful-increment-contract:v1.0"
      val expected = DockerImageName(
        image,
        Some(DefaultRegistryDomain),
        "we-sc/stateful-increment-contract",
        Some("v1.0"),
        None,
        s"$DefaultRegistryDomain/we-sc/stateful-increment-contract:v1.0"
      )
      DockerImageNameNormalizer.normalize(image, Some(DefaultRegistryDomain)) shouldBe Right(expected)
    }
  }
}

object NormalizeImageNameTestSuite {

  private val DefaultRegistryDomain = "default-registry-domain:5000"

}
