package enki.internal

trait DefaultsTo[Type, Default]

object DefaultsTo {
  implicit def defaultDefaultsTo[T]: DefaultsTo[T, T] = new DefaultsTo[T, T] {}
  implicit def fallback[T, D]: DefaultsTo[T, D] = new DefaultsTo[T, D] {}

  type Def[Default] = {
    type l[Type] = DefaultsTo[Type, Default]
  }
}
