package enki.oozie.model.workflow



import enki.oozie.types.WorkflowNode

import scala.xml.Node

final case class WorkflowKillAction(message: String = "Action failed, error message[${wf:errorMessage(wf:lastErrorNode())}]") extends WorkflowNode {
  override def toXml: Node = {
    <kill name="kill">
      <message>
        {message}
      </message>
    </kill>
  }
}