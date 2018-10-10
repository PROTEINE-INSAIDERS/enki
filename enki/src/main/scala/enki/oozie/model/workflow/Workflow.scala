package enki.oozie.model.workflow

import enki.oozie.types.WorkflowNode

import scala.xml.Node

final case class Workflow(
                           startTo: String,
                           workflowName: String,
                           action: WorkflowNode,
                           killAction: WorkflowKillAction,
                           endAction: WorkflowEndAction) extends WorkflowNode {

  override def toXml: Node = {
    <workflow-app name={workflowName} xmlns={xmlns}>
      <start to={startTo}/>
      {action.toXml}
      {killAction.toXml}
      {endAction.toXml}
    </workflow-app>

  }
}