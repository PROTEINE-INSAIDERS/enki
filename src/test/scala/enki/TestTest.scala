package enki

import cats._
import cats.implicits._
import enki.readers.default._
import enki.tests.EnkiSuite
import enki.writers.default._
import org.apache.spark.sql._
import org.scalatest.{Matchers, WordSpec}

import frameless.cats.implicits._
import cats.mtl.implicits._
import cats.effect._


import scala.reflect.io.Path
import frameless.TypedDataset
// import frameless.TypedDataset

import cats.data.ReaderT
// import cats.data.ReaderT

import cats.effect.IO
// import cats.effect.IO

import cats.effect.implicits._

class TestTest extends WordSpec with Matchers with EnkiSuite {

  type Action[T] = ReaderT[IO, SparkSession, T]

  "stages" in {


  }
}
