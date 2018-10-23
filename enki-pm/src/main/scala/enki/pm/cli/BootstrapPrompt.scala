package enki.pm.cli

import atto.Atto._
import atto.ParseResult._
import atto._
import cats._
import cats.data._
import cats.implicits._

class BootstrapPrompt[F[_] : Monad](
                                     implicit questions: Prompt[Const[String, ?]],
                                     parsers: Prompt[Lambda[a => Const[Parser[a], a]]],
                                     formatter: Formatter[F],
                                     console: Console[F]
                                   ) extends Prompt[F] {
  private def ask[A](
                      question: String,
                      answer: Either[Parser[A], A]
                    ): F[A] = {
    formatter.withInput(console.print(question)) *> console.print(" ") *> {
      answer match {
        case Right(res) => console.printLn(res.toString) *> res.pure[F]
        case Left(parser) =>
          def askUser: F[Either[String, A]] = console.readLine.map {
            parser.parseOnly(_) match {
              case Done(_, result) => Right(result)
              case Fail(_, _, msg) => Left(msg)
            }
          }

          askUser >>= {
            case Right(res) => res.pure[F]
            case Left(msg) => msg.tailRecM { msg =>
              formatter.withInvalidInput(console.printLn(msg)) *>
                formatter.withInput(console.print(question)) *>
                console.print(" ") *>
                askUser
            }
          }
      }
    }
  }

  override def projectName: F[String] = ask(questions.projectName.getConst, Left(parsers.projectName.getConst))

  override def sqlRoot: F[String] = ask(questions.sqlRoot.getConst, Right("sql"))

  override def projectDir: F[String] = ask(questions.projectDir.getConst, Right("/home/schernichkin/Projects/test-enki-project"))
}
