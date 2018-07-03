package enki

import enki.tests.EnkiSuite
import org.apache.spark.sql._
import org.scalatest.{Matchers, WordSpec}
// import frameless.TypedDataset

import cats.data.ReaderT
// import cats.data.ReaderT

import cats.effect.IO
// import cats.effect.IO

class TestTest extends WordSpec with Matchers with EnkiSuite {

  type Action[T] = ReaderT[IO, SparkSession, T]

  "stages" in {


  }
}
