package enki

import cats._
import cats.implicits._
import enki.default._

class GraphTest extends EnkiTestSuite {
  "getOpt" should {
    "return None if stage not found" in {
      ActionGraph.empty.getOpt("test") shouldBe None
    }

    "find stage by name" in {
      val emptyStage = ().pure[Stage]
      ActionGraph("test" , emptyStage ).getOpt("test") shouldBe Some(StageNode(emptyStage))
    }

    "find graph by name" in {
      ActionGraph("test" , ActionGraph.empty).getOpt("test") shouldBe Some(GraphNode(ActionGraph.empty))
    }
  }
}
