package enki.oozie.model.workflow

import enki.oozie.types.{WorkflowActionNode, WorkflowNode}

import scala.xml.Node

final case class WorkflowAction(action: WorkflowActionNode, endTo: String = "end", errorTo: String = "kill") extends WorkflowNode {

  override def toXml: Node = {
    <action name={action.name}>
      {action.toXml}
      <ok to={endTo}/>
      <error to={errorTo}/>
    </action>
  }

}
