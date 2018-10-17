package enki
package arg

import cats.mtl._

import scala.reflect.runtime.universe._

class ArgHandler[M[_]](implicit env: ApplicativeAsk[M, Parameters]) extends ArgAlg.Handler[M] {

  private def fromParameter[T: TypeTag](
                                         extractor: PartialFunction[ParameterValue, T],
                                         parameterValue: ParameterValue
                                       ): T = {
    extractor.lift(parameterValue) match {
      case Some(value) => value
      case None => throw new Exception(s"Invalid parameter type: required ${typeOf[T]} actual ${parameterValue.dataType}")
    }
  }

  private def fromParameterMap[T: TypeTag](
                                            name: String,
                                            defaultValue: Option[T],
                                            extractor: PartialFunction[ParameterValue, T]
                                          ): M[T] = {
    env.reader(p =>
      (p.parameters.get(name), defaultValue) match {
        case (Some(parameterValue), _) => fromParameter[T](extractor, parameterValue)
        case (None, Some(value)) => value
        case (None, None) => throw new Exception(s"Parameter `$name' not found.")
      })
  }

  override protected[this] def bool(
                                     name: String,
                                     description: String,
                                     defaultValue: Option[Boolean]): M[Boolean] = {
    fromParameterMap(name, defaultValue, { case BooleanValue(bool) => bool })
  }

  override protected[this] def int(
                                    name: String,
                                    description: String,
                                    defaultValue: Option[Int]
                                  ): M[Int] = {
    fromParameterMap(name, defaultValue, { case IntegerValue(int) => int })
  }

  override protected[this] def string(
                                       name: String,
                                       description: String,
                                       defaultValue: Option[String]): M[String] = {
    fromParameterMap(name, defaultValue, { case StringValue(str) => str })
  }
}