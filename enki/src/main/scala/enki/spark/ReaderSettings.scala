package enki.spark

case class ReaderSettings(partition: Seq[(String, String)]) {
  def setPartition(partition: (String, String)*): ReaderSettings = copy(partition = partition)
}

object ReaderSettings {
  def apply(): ReaderSettings = new ReaderSettings(Seq.empty)
}