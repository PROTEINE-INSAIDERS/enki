package enki.pm.cli

import cats._
import cats.effect._
import cats.implicits._

trait ConsoleTagFunctions {

  protected case class Tag(tag: String, style: Option[Style.Style])

  protected def withTag[F[_] : Applicative, A, E](tag: Tag)(f: F[A])
                                                 (implicit console: Console[F], bracket: Bracket[F, E]): F[A] =
    console.print("[") *>
      tag.style.map(console.withStyle(_)(console.print(tag.tag))).getOrElse(console.print(tag.tag)) *>
      console.print("]\t") *>
      f
}
