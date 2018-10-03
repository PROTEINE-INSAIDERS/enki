package enki

import scala.annotation.StaticAnnotation

//TODO: сделать систему автодокументирования
final class comment(val text: String) extends StaticAnnotation

final class decimalPrecision(val precision: Int, val scale: Int, val allowTruncate: Boolean = false) extends StaticAnnotation
