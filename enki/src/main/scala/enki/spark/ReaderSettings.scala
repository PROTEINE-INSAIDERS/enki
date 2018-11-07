package enki.spark

import org.apache.spark.sql.catalyst.TableIdentifier

case class ReaderSettings(
                           tableIdentifier: TableIdentifier,
                           partition: Seq[(String, String)]
                         ) {
  def setPartition(partition: (String, String)*): ReaderSettings = copy(partition = partition)
}

object ReaderSettings {
  def apply(tableIdentifier: TableIdentifier): ReaderSettings = ReaderSettings(tableIdentifier, Seq.empty)
}