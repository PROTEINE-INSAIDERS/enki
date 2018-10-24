package enki.pm.cli

import atto.Atto._
import atto.ParseResult._
import atto._
import cats._
import cats.data._
import cats.effect._
import cats.implicits._

class CliPrompt[F[_] : Monad, E](
                                  implicit questions: Prompt[Const[String, ?]],
                                  parsers: Prompt[Lambda[a => Const[Parser[a], a]]],
                                  console: Console[F],
                                  bracket: Bracket[F, E]
                                ) extends Prompt[F] with CliTag {
  private val inputTag = Tag("INPUT", Some(Style.CYAN))
  private val invalidInputTag = Tag("INVALID", Some(Style.MAGENTA))

  private def ask[A](
                      question: String,
                      answer: Either[Parser[A], A]
                    ): F[A] = {
    withTag(inputTag)(console.print(question)) *> console.print(" ") *> {
      answer match {
        case Right(res) => console.printLn(res.toString) *> res.pure[F]
        case Left(parser) =>
          def askUser: F[Either[String, A]] = console.readLine.map {
            parser.parseOnly(_) match {
              case Done(_, result) => Right(result)
              case Fail(_, _, msg) => Left(msg)
              case Partial(_) => throw new Exception("Unexpected partial result returned by parseOnly.")
            }
          }

          askUser >>= {
            case Right(res) => res.pure[F]
            case Left(msg) => msg.tailRecM { msg =>
              withTag(invalidInputTag)(console.printLn(msg)) *>
                withTag(inputTag)(console.print(question)) *>
                console.print(" ") *>
                askUser
            }
          }
      }
    }
  }

  override def projectName: F[String] = ask(questions.projectName.getConst, Left(parsers.projectName.getConst))

  override def sqlRoot: F[String] = ask(questions.sqlRoot.getConst, Right("sql"))

  override def projectDir: F[String] = ask(questions.projectDir.getConst, Right("~/Projects/test-enki-project"))
}
