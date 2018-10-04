package enki.oozie

import scala.util.Try
import scala.xml.Node

package object utils {
  def saveXml(path: String, xml: Node): Try[Unit] = Try(scala.xml.XML.save(path, xml))
}
