package enki.oozie.model.workflow.actions

import enki.oozie.types.WorkflowActionNode

import scala.xml.Node

final case class SparkAction(
                              actionName: String,
                              jobTracker: String,
                              nameNode: String,
                              master: String,
                              launcherName: String,
                              mainClass: String,
                              jar: String
                            ) extends WorkflowActionNode {

  override def name: String = actionName

  override def xmlns: String = "uri:oozie:spark-action:1.0"

  override def toXml: Node = {
    <spark xmlns={xmlns}>
      <job-tracker>
        {jobTracker}
      </job-tracker>
      <name-node>
        {nameNode}
      </name-node>
      <master>
        {master}
      </master>
      <name>
        {launcherName}
      </name>
      <class>
        {mainClass}
      </class>
      <jar>
        {jar}
      </jar>
    </spark>
  }

}
