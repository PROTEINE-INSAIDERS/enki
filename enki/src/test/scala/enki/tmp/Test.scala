package enki.tmp

import cats._
import cats.data._
import cats.implicits._
import enki.DataFrameWriterState
import enki.arguments.Arguments
import enki.tmp.test.test1
import enki.writer._
import freestyle.free._
import org.apache.spark.sql.SaveMode

@module trait Yoba {
  val argument: enki.arguments.Arguments
  val writer: enki.writer.DataFrameWriter
}

object test {
  def test2[F[_]](implicit writer: DataFrameWriter[F]): FreeS.Par[F, Unit] = {


    val aa = ().pure[writer.FS]
    aa
  }

  def test[F[_]](implicit yoba: Yoba[F]): yoba.FS[Int] = {
    import yoba._

    val kk = (argument.int("test"), writer.format("test")) mapN { (a: Int, b: Unit) => a }
    kk
  }

  def test1() = {
    implicit val h1 = new Arguments.Handler[Const[List[String], ?]] {
      override def string(name: String, description: String, defaultValue: Option[String]): Const[List[String], String] =
        ???

      override def int(name: String, description: String, defaultValue: Option[Int]): Const[List[String], Int] =
        ???
    }

    implicit val h2 = new enki.writer.DataFrameWriter.Handler[Const[List[String], ?]] {
      override protected[this] def mode(saveMode: SaveMode): Const[List[String], Unit] = ???

      override protected[this] def format(source: String): Const[List[String], Unit] = ???

      override protected[this] def partitionBy(colNames: Seq[String]): Const[List[String], Unit] = ???
    }

    //TODO: manual handler??

    val aaaa = test[Yoba.Op].analyze[Set[String]](λ[Yoba.Op ~> λ[α => Set[String]]] {
      //      case IntOp(_, _, _) => println("========")
      case a =>
        println(a)
        Set.empty
    })
  }

}

object TTTTTTTTTTTT {
  def main(args: Array[String]): Unit = {
    val ddd = test.test2

    println(ddd)

    implicit val configurator = new DataFrameWriterConfigurator[(Int, Int)]()

    val aaaaa = ddd.interpret[DataFrameWriterState[(Int, Int), ?]]
  }

}

/*
case class Customer(id: String, name: String)

case class Order(crates: Int, variety: String, customerId: String)

case class Config(varieties: Set[String])

object algebras {

  @free trait CustomerPersistence {
    def getCustomer(id: String): FS[Option[Customer]]
  }

  @free trait StockPersistence {
    def checkQuantityAvailable(variety: String): FS[Int]

    def registerOrder(order: Order): FS[Unit]
  }

}

object modules {
  val rd = reader[Config]
  val cacheP = new KeyValueProvider[String, Customer]

  @module trait Persistence {
    val customer: algebras.CustomerPersistence
    val stock: algebras.StockPersistence
  }

  @module trait App {
    val persistence: Persistence

    val errorM: ErrorM
    val cacheM: cacheP.CacheM
    val readerM: rd.ReaderM
  }

}

object yy {
  import modules._

  def validateOrder[F[_]](order: Order, customer: Customer)(implicit app: App[F]): FreeS.Par[F, ValidatedNel[String, Unit]] =
    app.readerM.reader { config =>
      val v = ().validNel[String]
      v.ensure(NonEmptyList.one(
        "Number of crates ordered should be bigger than zero"))(
        _ => order.crates > 0) |+|
        v.ensure(NonEmptyList.one(
          "Apple variety is not available"))(
          _ => config.varieties.contains(order.variety.toLowerCase))
    }
}
*/