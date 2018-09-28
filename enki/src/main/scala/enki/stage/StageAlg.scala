package enki
package stage

import freestyle.free._
import org.apache.spark.sql._
import org.apache.spark.sql.types._

trait StageAlg[FF$334[_]] extends _root_.freestyle.free.internal.EffectLike[FF$334] {
  def dataFrame(rows: Seq[Row], schema: StructType): FS[DataFrame]

  def dataset[T](data: Seq[T], encoder: Encoder[T]): FS[Dataset[T]]

  def readDataFrame(schemaName: String, tableName: String): FS[DataFrame]

  def readDataset[T](schemaName: String, tableName: String, encoder: Encoder[T], strict: Boolean): FS[Dataset[T]]

  //TODO: Settings, помещенные сюда становятся невидимыми для интерпретатора, а мы, возможно, захотим их менять.
  def writeDataFrame(schemaName: String, tableName: String): FS[WriterSettings[Row] => DataFrame => Unit]

  def writeDataset[T](schemaName: String, tableName: String, encoder: Encoder[T], strict: Boolean): FS[WriterSettings[T] => Dataset[T] => Unit]
}

@_root_.java.lang.SuppressWarnings(_root_.scala.Array("org.wartremover.warts.Any", "org.wartremover.warts.AsInstanceOf", "org.wartremover.warts.Throw")) object StageAlg {

  sealed trait Op[_] extends _root_.scala.Product with _root_.java.io.Serializable {
    val FSAlgebraIndex344: _root_.scala.Int
  }

  final case class DataFrameOp(val rows: Seq[Row], val schema: StructType) extends _root_.scala.AnyRef with Op[DataFrame] {
    override val FSAlgebraIndex344: _root_.scala.Int = 0
  }

  final case class DatasetOp[T](val data: Seq[T], val encoder: Encoder[T]) extends _root_.scala.AnyRef with Op[Dataset[T]] {
    override val FSAlgebraIndex344: _root_.scala.Int = 1
  }

  final case class ReadDataFrameOp(val schemaName: String, val tableName: String) extends _root_.scala.AnyRef with Op[DataFrame] {
    override val FSAlgebraIndex344: _root_.scala.Int = 2
  }

  final case class ReadDatasetOp[T](val schemaName: String, val tableName: String, val encoder: Encoder[T], val strict: Boolean) extends _root_.scala.AnyRef with Op[Dataset[T]] {
    override val FSAlgebraIndex344: _root_.scala.Int = 3
  }

  final case class WriteDataFrameOp(val schemaName: String, val tableName: String) extends _root_.scala.AnyRef with Op[WriterSettings[Row] => DataFrame => Unit] {
    override val FSAlgebraIndex344: _root_.scala.Int = 4
  }

  final case class WriteDatasetOp[T](val schemaName: String, val tableName: String, val encoder: Encoder[T], val strict: Boolean) extends _root_.scala.AnyRef with Op[WriterSettings[T] => Dataset[T] => Unit] {
    override val FSAlgebraIndex344: _root_.scala.Int = 5
  }

  type OpTypes = _root_.iota.TConsK[Op, _root_.iota.TNilK]

  trait Handler[MM$358[_]] extends _root_.freestyle.free.FSHandler[Op, MM$358] {
    protected[this] def dataFrame(rows: Seq[Row], schema: StructType): MM$358[DataFrame]

    protected[this] def dataset[T](data: Seq[T], encoder: Encoder[T]): MM$358[Dataset[T]]

    protected[this] def readDataFrame(schemaName: String, tableName: String): MM$358[DataFrame]

    protected[this] def readDataset[T](schemaName: String, tableName: String, encoder: Encoder[T], strict: Boolean): MM$358[Dataset[T]]

    protected[this] def writeDataFrame(schemaName: String, tableName: String): MM$358[WriterSettings[Row] => DataFrame => Unit]

    protected[this] def writeDataset[T](schemaName: String, tableName: String, encoder: Encoder[T], strict: Boolean): MM$358[WriterSettings[T] => Dataset[T] => Unit]

    type PP$337
    type PP$340
    type PP$343

    override def apply[AA$359](fa$378: Op[AA$359]): MM$358[AA$359] = ((fa$378.FSAlgebraIndex344: @_root_.scala.annotation.switch) match {
      case 0 =>
        val fresh381: DataFrameOp = fa$378.asInstanceOf[DataFrameOp]
        dataFrame(fresh381.rows, fresh381.schema)
      case 1 =>
        val fresh382: DatasetOp[PP$337] = fa$378.asInstanceOf[DatasetOp[PP$337]]
        dataset(fresh382.data, fresh382.encoder)
      case 2 =>
        val fresh383: ReadDataFrameOp = fa$378.asInstanceOf[ReadDataFrameOp]
        readDataFrame(fresh383.schemaName, fresh383.tableName)
      case 3 =>
        val fresh384: ReadDatasetOp[PP$340] = fa$378.asInstanceOf[ReadDatasetOp[PP$340]]
        readDataset(fresh384.schemaName, fresh384.tableName, fresh384.encoder, fresh384.strict)
      case 4 =>
        val fresh385: WriteDataFrameOp = fa$378.asInstanceOf[WriteDataFrameOp]
        writeDataFrame(fresh385.schemaName, fresh385.tableName)
      case 5 =>
        val fresh386: WriteDatasetOp[PP$343] = fa$378.asInstanceOf[WriteDatasetOp[PP$343]]
        writeDataset(fresh386.schemaName, fresh386.tableName, fresh386.encoder, fresh386.strict)
      case i =>
        throw new _root_.java.lang.Exception("freestyle internal error: index " + i.toString() + " out of bounds for " + this.toString())
    }).asInstanceOf[MM$358[AA$359]]
  }

  class To[LL$349[_]](implicit ii$350: _root_.freestyle.free.InjK[Op, LL$349]) extends StageAlg[LL$349] {
    private[this] val toInj351 = _root_.freestyle.free.FreeS.inject[Op, LL$349](ii$350)

    override def dataFrame(rows: Seq[Row], schema: StructType): FS[DataFrame] = toInj351(DataFrameOp(rows, schema))

    override def dataset[T](data: Seq[T], encoder: Encoder[T]): FS[Dataset[T]] = toInj351(DatasetOp[T](data, encoder))

    override def readDataFrame(schemaName: String, tableName: String): FS[DataFrame] = toInj351(ReadDataFrameOp(schemaName, tableName))

    override def readDataset[T](schemaName: String, tableName: String, encoder: Encoder[T], strict: Boolean): FS[Dataset[T]] = toInj351(ReadDatasetOp[T](schemaName, tableName, encoder, strict))

    override def writeDataFrame(schemaName: String, tableName: String): FS[WriterSettings[Row] => DataFrame => Unit] = toInj351(WriteDataFrameOp(schemaName, tableName))

    override def writeDataset[T](schemaName: String, tableName: String, encoder: Encoder[T], strict: Boolean): FS[WriterSettings[T] => Dataset[T] => Unit] = toInj351(WriteDatasetOp[T](schemaName, tableName, encoder, strict))
  }

  implicit def to[LL$349[_]](implicit ii$350: _root_.freestyle.free.InjK[Op, LL$349]): To[LL$349] = new To[LL$349]

  def apply[FF$334[_]](implicit ev$387: StageAlg[FF$334]): StageAlg[FF$334] = ev$387

  def instance(implicit ev: StageAlg[Op]): StageAlg[Op] = ev
}

/*
sealed trait StageAction[T]

final case class DataFrameAction(rows: Seq[Row], schema: StructType) extends StageAction[DataFrame]

final case class DatasetAction[T](data: Seq[T], encoder: Encoder[T]) extends StageAction[Dataset[T]]

final case class WriteDataFrameAction(
                                       schemaName: String,
                                       tableName: String,
                                       writerSettings: FreeS.Par[DataFrameWriter.Op, Unit]
                                     ) extends StageAction[DataFrame => Unit] with WriteTableAction

final case class WriteDatasetAction[T](
                                        schemaName: String,
                                        tableName: String,
                                        encoder: Encoder[T],
                                        strict: Boolean,
                                        writerSettings: FreeS.Par[DataFrameWriter.Op, Unit]
                                      ) extends StageAction[Dataset[T] => Unit] with WriteTableAction

*/