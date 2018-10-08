package enki

import cats._
import cats.data._
import cats.free._

package object internal {

  /**
    * Convert constant interpreter to analyzer.
    */
  implicit class InterpreterToAnalyzer[F[_], M](f: F ~> Const[M, ?]) {
    def analyzer: F ~> λ[α => M] = λ[F ~> λ[α => M]] {
      case a => f(a).getConst
    }
  }

  implicit class EnkiFreeApplicativeExtensions[F[_], A](fa: FreeApplicative[F, A]) {
    /**
      * Analyze injected algebra using monoidal analyzer.
      */
    def analyzeIn[G[_], M: Monoid](
                                    f: G ~> λ[α => M]
                                  )(
                                    implicit in: InjectK[G, F]
                                  ): M = {
      fa.analyze(λ[F ~> λ[α => M]] {
        case in(a) => f(a)
        case _ => Monoid.empty[M]
      })
    }
  }

}
