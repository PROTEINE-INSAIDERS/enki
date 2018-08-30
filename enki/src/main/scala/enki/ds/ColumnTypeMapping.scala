package enki.ds

trait ColumnTypeMapping[X, Y]

trait IdentityTypeMapping {
  implicit def identityColumnTypeMapping[X]: ColumnTypeMapping[X, X] = new ColumnTypeMapping[X, X] {}
}

trait OptionTypeMapping extends IdentityTypeMapping {
  implicit def optionColumnTypeMapping[T[_] <: Option[_], R]: ColumnTypeMapping[T[R], R] = new ColumnTypeMapping[T[R], R] {}
}
