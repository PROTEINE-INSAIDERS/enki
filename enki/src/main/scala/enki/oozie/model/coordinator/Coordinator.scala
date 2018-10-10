package enki.oozie.model.coordinator

import enki.oozie.types.CoordinatorNode

import scala.xml.Node

case class Coordinator(
                        coordinatorName: String,
                        frequency: String,
                        startTime: String,
                        endTime: String,
                        timezone: String,
                        hdfsWorkflowPath: String
                      ) extends CoordinatorNode {
  override def toXml: Node = {
    <coordinator-app name={coordinatorName} frequency={frequency} start={startTime} end={endTime} timezone={timezone} xmlns={xmlns}>
      <action>
        <workflow>
          <app-path>
            {hdfsWorkflowPath}
          </app-path>
        </workflow>
      </action>
    </coordinator-app>
  }
}
