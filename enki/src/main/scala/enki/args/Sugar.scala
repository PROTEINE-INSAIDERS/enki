package enki.args

import scala.reflect.runtime.universe._

// весь сахар, который мы пробрасываем, зависит от конктретного типа операций, поэтому можно здесь его параметризовать этим типом.
trait Sugar[F[_]] {
  final def arg[T: TypeTag](
                             name: String,
                             description: String = "",
                             defaultValue: Option[T] = None
                           )
                           (
                             implicit argsAlg: ArgsAlg[F]
                           ): argsAlg.FS[T] = {
    if (typeOf[T] == typeOf[Boolean]) {
      argsAlg.bool(name, description, defaultValue.asInstanceOf[Option[Boolean]]).asInstanceOf[argsAlg.FS[T]]
    } else if (typeOf[T] == typeOf[Int]) {
      argsAlg.int(name, description, defaultValue.asInstanceOf[Option[Int]]).asInstanceOf[argsAlg.FS[T]]
    } else if (typeOf[T] == typeOf[String]) {
      argsAlg.string(name, description, defaultValue.asInstanceOf[Option[String]]).asInstanceOf[argsAlg.FS[T]]
    } else {
      throw new Exception(s"Argument of type ${typeOf[T]} not supported.")
    }
  }
}
