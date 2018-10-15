package enki

import scala.annotation._

//TODO: сделать систему автодокументирования
final class comment(val text: String) extends StaticAnnotation

//TODO: Fix fucking annotations
// https://stackoverflow.com/questions/35251426/what-is-the-current-state-of-scala-reflection-capabilities-especially-wrt-ann/35254466#35254466
final class decimalPrecision(val precision: Int, val scale: Int, val allowTruncate: Boolean = false) extends ClassfileAnnotation
