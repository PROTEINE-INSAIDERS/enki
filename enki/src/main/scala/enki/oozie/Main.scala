package enki.oozie

import enki.oozie.model.coordinator.Coordinator
import enki.oozie.model.workflow.{Workflow, WorkflowEndAction, WorkflowKillAction, WorkflowSequenceActions}
import enki.oozie.model.workflow.actions.SparkAction
import enki.oozie.validator.{CoordinatorValidator, WorkflowValidator}
import enki.oozie.utils._


object Main {


  def main(args: Array[String]): Unit = {
    simpleCoordinator()

  }

  def simpleCoordinator(): Unit = {
    val xmlCoordinatorPath = "enki/src/main/xml/coordinator.xml"

    val coordinator = Coordinator(
      coordinatorName = "coordinatorName",
      frequency = "${coord:days(1)}",
      startTime = "2017-06-18T12:50Z",
      endTime = "2018-06-18T12:15Z",
      timezone = "Europe/London",
      hdfsWorkflowPath = "hdfs://customPath/"
    ).toXml


    println(coordinator)

    saveXml(xmlCoordinatorPath, coordinator)

    println(
      CoordinatorValidator.validate(xmlCoordinatorPath)
    )
  }

  def simpleWorkflow(): Unit = {

    val xmlWorkflowPath = "enki/src/main/xml/workflow.xml"

    val sparkAction1 = SparkAction(
      actionName = "spark-action1",
      jobTracker = "jobTracker",
      nameNode = "nameNode",
      master = "master",
      launcherName = "launcherName",
      mainClass = "mainClass",
      jar = "jar"
    )

    val sparkAction2 = SparkAction(
      actionName = "spark-action2",
      jobTracker = "jobTracker",
      nameNode = "nameNode",
      master = "master",
      launcherName = "launcherName",
      mainClass = "mainClass",
      jar = "jar"
    )

    val workflow = Workflow(
      startTo = "spark-action1",
      workflowName = "workflow-action",
      action = WorkflowSequenceActions(List(sparkAction1, sparkAction2)),
      killAction = WorkflowKillAction(),
      endAction = WorkflowEndAction()
    ).toXml

    println(workflow)

    saveXml(xmlWorkflowPath, workflow)

    println(
      WorkflowValidator.validate(xmlWorkflowPath)
    )
  }

}
