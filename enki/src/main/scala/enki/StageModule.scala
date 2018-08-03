package enki

import cats._
import cats.free.FreeApplicative
import cats.free.FreeApplicative._
import cats.implicits._
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.encoders._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.objects.StaticInvoke
import org.apache.spark.sql.types._

import scala.collection.JavaConversions._
import scala.reflect.runtime.currentMirror
import scala.reflect.runtime.universe._
import scala.tools.reflect.ToolBox

trait StageModule {

  sealed trait StageAction[T]

  //TODO: ReadAction и WriteAction могут содержать много настроечных параметров. Необходимо вынести их в ReaderSettings и
  // WriterSettings, чтобы в дальнейшем уменьнить количество рефакторингов при добавлении новых параметров.
  final case class ReadAction[T: TypeTag](
                                           schemaName: String,
                                           tableName: String,
                                           strict: Boolean
                                         ) extends StageAction[Dataset[T]] {
    private lazy val toolbox = currentMirror.mkToolBox()

    private lazy val expressionEncoder: ExpressionEncoder[T] = ExpressionEncoder[T]()

    //TODO: move to metadata module??
    private lazy val bigDecimalReplacements: Map[String, DecimalType] = {
      def instantiate(annotation: Annotation): Any = {
        toolbox.eval(toolbox.untypecheck(annotation.tree))
      }

      symbolOf[T].asClass.primaryConstructor.typeSignature.paramLists.head
        .map { symbol =>
          symbol.name.toString -> symbol.annotations.filter(_.tree.tpe <:< typeOf[decimalPrecision]).map(instantiate(_).asInstanceOf[decimalPrecision])
        }
        .flatMap {
          case (name, a :: Nil) => Some((name, DecimalType(a.precision, a.scale)))
          case (_, Nil) => None
          case (name, _) => throw new Exception(s"Multiple ${typeOf[decimalPrecision]} annotation applied to $name.")
        }.toMap
    }

    //TODO: support nested classes.
    private lazy val strictEncoder: ExpressionEncoder[T] = {
      val schema = StructType(expressionEncoder.schema.map {
        case f: StructField if bigDecimalReplacements.contains(f.name) => f.copy(dataType = bigDecimalReplacements(f.name))
        case other => other
      })

      val serializer = expressionEncoder.serializer.map {
        case a: Alias if bigDecimalReplacements.contains(a.name) =>
          a.transform {
            case s: StaticInvoke =>
              s.copy(dataType = bigDecimalReplacements(a.name))
          }
        case other => other
      }

      val deserializer = expressionEncoder.deserializer.transform {
        case u@UpCast(a@UnresolvedAttribute(_), _, _) if bigDecimalReplacements.contains(a.name) =>
          u.copy(dataType = bigDecimalReplacements(a.name))
      }

      expressionEncoder.copy(schema = schema, serializer = serializer, deserializer = deserializer)
    }

    private[enki] def apply(session: SparkSession): Dataset[T] = {
      if (typeOf[T] == typeOf[Row]) {
        if (strict) {
          throw new Exception("Unable to restrict schema for generic type Row.")
        }
        session.table(s"$schemaName.$tableName").asInstanceOf[Dataset[T]]
      }
      else {
        val table = session.table(s"$schemaName.$tableName")
        if (strict) {
          table.select(strictEncoder.schema.map(f => table(f.name)): _*).as[T](strictEncoder)
        } else {
          table.as[T](expressionEncoder)
        }
      }
    }

    def tag: TypeTag[T] = implicitly
  }

  final case class WriteAction[T](
                                   schemaName: String,
                                   tableName: String,
                                   saveMode: Option[SaveMode]
                                 ) extends StageAction[Dataset[T] => Unit]

  final case class DatasetAction[T: TypeTag](data: Seq[T]) extends StageAction[Dataset[T]] {
    val tag: TypeTag[T] = implicitly
  }

  final case class DataFrameAction(rows: Seq[Row], schema: StructType) extends StageAction[DataFrame]

  type Stage[A] = FreeApplicative[StageAction, A]

  def dataFrame(rows: Seq[Row], schema: StructType): Stage[DataFrame] =
    lift[StageAction, DataFrame](DataFrameAction(rows, schema))

  def dataset[T: TypeTag](data: Seq[T]): Stage[Dataset[T]] =
    lift[StageAction, Dataset[T]](DatasetAction(data))

  def read[T: TypeTag](
                        schemaName: String,
                        tableName: String,
                        restricted: Boolean
                      ): Stage[Dataset[T]] =
    lift[StageAction, Dataset[T]](ReadAction(schemaName, tableName, restricted))

  def write[T](
                schemaName: String,
                tableName: String,
                saveMode: Option[SaveMode]
              ): Stage[Dataset[T] => Unit] =
    lift[StageAction, Dataset[T] => Unit](WriteAction(schemaName, tableName, saveMode))

  def stageCompiler: StageAction ~> SparkAction = λ[StageAction ~> SparkAction] {
    case action: DatasetAction[t] => session: SparkSession =>
      session.createDataset(action.data)(ExpressionEncoder()(action.tag))

    case action: ReadAction[t] => action(_)

    case action: WriteAction[t] => _: SparkSession =>
      (dataset: Dataset[t]) => {
        val writer = dataset.write

        action.saveMode.foreach(writer.mode)
        writer.saveAsTable(s"${action.schemaName}.${action.tableName}")
      }

    case action: DataFrameAction => session: SparkSession =>
      session.createDataFrame(action.rows, action.schema)
  }

  def stageReads(stage: Stage[_]): Set[ReadAction[_]] = {
    stage.analyze(λ[StageAction ~> λ[α => Set[ReadAction[_]]]] {
      case r: ReadAction[_] => Set(r)
      case _ => Set.empty
    })
  }

  def stageWrites(stage: Stage[_]): Set[WriteAction[_]] = {
    stage.analyze(λ[StageAction ~> λ[α => Set[WriteAction[_]]]] {
      case r: WriteAction[_] => Set(r)
      case _ => Set.empty
    })
  }

  def stageNonEmpty(stage: Stage[_]): Boolean = {
    stage.analyze(λ[StageAction ~> λ[α => Option[Unit]]] {
      case _ => Some(Unit)
    }).nonEmpty
  }
}