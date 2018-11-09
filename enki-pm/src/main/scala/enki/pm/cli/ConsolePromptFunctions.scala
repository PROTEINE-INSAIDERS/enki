package enki.pm.cli

import atto.Atto._
import atto.ParseResult._
import atto._
import cats._
import cats.effect._
import cats.implicits._

trait ConsolePromptFunctions extends ConsoleTagFunctions {
  protected val input = Tag("INPUT", Some(Style.CYAN))
  protected val invalidInput = Tag("INVALID", Some(Style.MAGENTA))
  protected val auto = Tag("AUTO", Some(Style.GREEN))

  protected def ask[F[_] : Monad, A, E](
                                         question: String,
                                         parser: Parser[A]
                                       )
                                       (
                                         implicit console: Console[F],
                                         bracket: Bracket[F, E],
                                         recorder: AnswerRecorder[F]
                                       ): F[A] = for {
    _ <- withTag(input)(console.print(question)) *> console.print(" ")
    recorded <- recorder.get(question)
    in <- recorded match {
      case Some(a) => withTag(auto)(console.printLn(a)) *> a.pure[F]
      case None => console.readLine
    }
    result <- in.tailRecM { in =>
      parser.parseOnly(in) match {
        case Done(_, result) =>
          recorder.store(question, in) *> Either.right[String, A](result).pure[F]
        case Fail(_, _, msg) => for {
          _ <- withTag(invalidInput)(console.printLn(msg))
          _ <- withTag(input)(console.print(question)) *> console.print(" ")
          in <- console.readLine
        } yield Either.left[String, A](in)
        case Partial(_) =>
          throw new InternalError("Unexpected partial result returned by parseOnly.")
      }
    }
  } yield result
}
