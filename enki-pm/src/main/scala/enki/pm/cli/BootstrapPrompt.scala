package enki.pm.cli

import cats._
import cats.data._
import cats.implicits._

import atto._, Atto._
import cats.implicits._

class BootstrapPrompt[F[_] : Applicative](
                                           implicit questions: Prompt[Const[String, ?]],
                                           formatter: Formatter[F],
                                           console: Console[F]
                                         ) extends Prompt[F] {
  private def ask[A](question: String, answer: Option[A]): F[A] = {
    formatter.withQuestion(console.print(question)) *> console.print(" ") *> {
      answer match {
        case Some(answer) => console.printLn(answer.toString) *> answer.pure[F]
        case None => ??? // TODO: добавить tpolecat/atto, распарсить ответ в A,
        // если ошибка, спросить снова, иначе вернуть ответ (потребует замену Applicative на Monad).
      }
    }
  }

  override def projectName: F[String] = ???

  override def whereDoYouWantToGoToday: F[String] = {
    ask(questions.whereDoYouWantToGoToday.getConst , Some("Microsoft®"))
  }
}

