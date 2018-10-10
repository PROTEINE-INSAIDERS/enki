package enki.oozie

import scala.xml.NodeSeq

package object types {

  trait OozieTask

  trait OozieNode

  trait WorkflowNode extends OozieNode {
    def xmlns: String = "uri:oozie:workflow:1.0"

    def toXml: NodeSeq
  }

  trait WorkflowActionNode extends OozieNode {
    def name: String

    def xmlns: String

    def toXml: NodeSeq
  }

  trait CoordinatorNode extends OozieNode {
    def xmlns: String = "uri:oozie:coordinator:0.5"

    def toXml: NodeSeq
  }

}
