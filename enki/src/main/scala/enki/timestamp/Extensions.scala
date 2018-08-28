package enki
package timestamp

import java.sql._
import java.time._
import java.time.temporal.ChronoUnit._

trait Extensions {

  implicit class TimestampExtensions(timestamp: Timestamp) {
    private def adjust(f: LocalDateTime => LocalDateTime): Timestamp = Timestamp.valueOf(f(timestamp.toLocalDateTime))

    private def project[T](f: LocalDateTime => T): T = f(timestamp.toLocalDateTime)


    def atMidnight: Timestamp = adjust(time => LocalDateTime.of(time.toLocalDate, LocalTime.MIDNIGHT))

    def atNoon: Timestamp = adjust(time => LocalDateTime.of(time.toLocalDate, LocalTime.NOON))

    def atStartOfDay: Timestamp = adjust(_.toLocalDate.atStartOfDay())

    def atStartOfMonth: Timestamp = adjust(_.toLocalDate.withDayOfMonth(1).atStartOfDay())

    def atStartOfYear: Timestamp = adjust(time => LocalDate.of(time.getYear, Month.JANUARY, 1).atStartOfDay())


    def isStartOfDay: Boolean = project(_.toLocalTime == LocalTime.MIDNIGHT)


    def year: Int = project(_.getYear)

    def quarterOfYear: Int = project(date => (date.getMonthValue - 1) / 4 + 1)

    def monthOfYear: Int = project(date => date.getMonthValue)


    def daysBetween(other: Timestamp): Long = DAYS.between(timestamp.toLocalDateTime, other.toLocalDateTime)

    def monthsBetween(other: Timestamp): Long = MONTHS.between(timestamp.toLocalDateTime, other.toLocalDateTime)


    def minusDays(days: Long): Timestamp = adjust(_.minusDays(days))

    def minusMonths(months: Long): Timestamp = adjust(_.minusMonths(months))

    def minusYears(years: Long): Timestamp = adjust(_.minusYears(years))


    def plusDays(days: Long): Timestamp = adjust(_.plusDays(days))

    def plusMonth(months: Long): Timestamp = adjust(_.plusMonths(months))

    def plusYears(years: Long): Timestamp = adjust(_.plusYears(years))
  }

}
