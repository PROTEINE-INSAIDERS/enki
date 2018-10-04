package enki.oozie.model.workflow

import enki.oozie.types.WorkflowNode

import scala.xml.Node

final case class WorkflowEndAction() extends WorkflowNode {
  override def toXml: Node = {
      <end name="end"/>
  }
}