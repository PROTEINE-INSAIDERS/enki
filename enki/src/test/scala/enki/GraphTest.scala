package enki

class GraphTest extends EnkiTestSuite {
  "getOpt" should {
    "return None if stage not found" in {
      ActionGraph.empty.getOpt("test") shouldBe None
    }

    "find stage by name" in {
      ActionGraph("test" , emptyStage).getOpt("test") shouldBe Some(Right(emptyStage))
    }

    "find graph by name" in {
      ActionGraph("test" , ActionGraph.empty).getOpt("test") shouldBe Some(Left(ActionGraph.empty))
    }
  }
}
