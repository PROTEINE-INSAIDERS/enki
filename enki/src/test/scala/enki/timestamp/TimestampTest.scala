package enki.timestamp

import java.sql.Timestamp

import org.scalatest.{Matchers, WordSpec}

class TimestampTest extends WordSpec with Matchers {
  "daysBetween" in {
    val a = Timestamp.valueOf("2018-01-01 00:00:00")
    val b = Timestamp.valueOf("2018-01-02 00:00:00")
    (a daysBetween b) shouldBe 1
  }

  "monthsBetween" in {
    val a = Timestamp.valueOf("2018-01-01 00:00:00")
    val b = Timestamp.valueOf("2018-02-01 00:00:00")
    (a monthsBetween  b) shouldBe 1
  }

  "interpolator" should {
    "parse date-time" in {
      timestamp"2018-02-01 15:30:00" shouldBe Timestamp.valueOf("2018-02-01 15:30:00")
    }

    "parse date" in {
      timestamp"2018-02-01" shouldBe Timestamp.valueOf("2018-02-01 00:00:00")
    }
  }
}
