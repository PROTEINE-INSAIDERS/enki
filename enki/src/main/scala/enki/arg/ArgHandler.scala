package enki
package arg

import cats.data._

import scala.reflect.runtime.universe._

class ArgHandler extends ArgAlg.Handler[EnkiMonad] {

  private def fromParameter[T: TypeTag](
                                         extractor: PartialFunction[ParameterValue, T],
                                         parameterValue: ParameterValue
                                       ): T = {
    extractor.lift(parameterValue) match {
      case Some(value) => value
      case None => throw new Exception(s"Invalid parameter type: required ${typeOf[T]} actual ${parameterValue.dataType}")
    }
  }

  private def fromParameter[T: TypeTag](parameterValue: ParameterValue): T = {
    if (typeOf[T] == typeOf[Int]) {
      fromParameter({ case IntegerValue(int) => int }, parameterValue).asInstanceOf[T]
    } else if (typeOf[T] == typeOf[String]) {
      fromParameter({ case StringValue(str) => str }, parameterValue).asInstanceOf[T]
    } else if (typeOf[T] == typeOf[Boolean]) {
      fromParameter({ case BooleanValue(bool) => bool }, parameterValue).asInstanceOf[T]
    }
    else {
      throw new Exception(s"Argument type ${typeOf[T]} not supported.")
    }
  }

  private def fromParameterMap[T: TypeTag](
                                            parameters: Map[String, ParameterValue],
                                            name: String,
                                            defaultValue: Option[T]
                                          ): T = {
    (parameters.get(name), defaultValue) match {
      case (Some(parameterValue), _) => fromParameter[T](parameterValue)
      case (None, Some(value)) => value
      case (None, None) => throw new Exception(s"Parameter `$name' not found.")
    }
  }

  override protected[this] def bool(
                                     name: String,
                                     description: String,
                                     defaultValue: Option[Boolean]): Reader[Environment, Boolean] = Reader { env =>
    fromParameterMap[Boolean](env.parameters, name, defaultValue)
  }

  override protected[this] def int(
                                    name: String,
                                    description: String,
                                    defaultValue: Option[Int]
                                  ): Reader[Environment, Int] = Reader { env =>
    fromParameterMap[Int](env.parameters, name, defaultValue)
  }

  override protected[this] def string(
                                       name: String,
                                       description: String,
                                       defaultValue: Option[String]): Reader[Environment, String] = Reader { env =>
    fromParameterMap[String](env.parameters, name, defaultValue)
  }
}

object ArgHandler extends ArgHandler