package enki

import java.sql.Timestamp
import java.time.LocalDate

import contextual._

import scala.util.control.NonFatal

package object timestamp extends TimestampModule {

  object TimestampInterpolator extends Interpolator {
    type Output = Timestamp

    override def contextualize(interpolation: StaticInterpolation): Seq[ContextType] = Nil

    override def evaluator(contexts: Seq[ContextType], interpolation: StaticInterpolation): interpolation.universe.Tree = {
      import interpolation.universe.{Literal => _, _}

      val time = interpolation.parts.head match {
        case lit@Literal(index, string) =>
          try {
            val timestamp = if (string.length >= 8 && string.length <= 10) {
              Timestamp.valueOf(LocalDate.parse(string).atStartOfDay())
            } else {
              Timestamp.valueOf(string)
            }
            timestamp.getTime
          }
          catch {
            case NonFatal(e) => interpolation.abort(lit, 0, e.getMessage)
          }
        case hole@Hole(_, _) =>
          interpolation.abort(hole, "can't make substitutions")
      }
      q"""new _root_.java.sql.Timestamp($time)"""
    }
  }

  implicit class TimestampStringContext(sc: StringContext) {
    val timestamp = Prefix(TimestampInterpolator, sc)
  }

}