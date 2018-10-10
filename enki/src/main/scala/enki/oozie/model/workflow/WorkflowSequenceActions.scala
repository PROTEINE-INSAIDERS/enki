package enki.oozie.model.workflow

import enki.oozie.types.{WorkflowActionNode, WorkflowNode}

import scala.xml.NodeSeq

final case class WorkflowSequenceActions(actions: List[WorkflowActionNode], endTo: String = "end", errorTo: String = "kill") extends WorkflowNode {
  override def toXml: NodeSeq = {

    def run(actions: List[WorkflowActionNode]): NodeSeq = actions match {
      case h :: Nil =>
        <action name={h.name}>
          {h.toXml}
          <ok to={endTo}/>
          <error to={errorTo}/>
        </action>

      case h :: t =>
        <action name={h.name}>
          {h.toXml}
          <ok to={t.head.name}/>
          <error to={errorTo}/>
        </action> ++
          run(t)

    }

    run(actions)
  }

}
