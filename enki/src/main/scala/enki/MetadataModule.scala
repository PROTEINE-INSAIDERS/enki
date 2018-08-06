package enki

import scala.annotation.StaticAnnotation


trait MetadataModule {

  //TODO: сделать систему автодокументирования
  final class comment(val value: String) extends StaticAnnotation

  final class decimalPrecision(val precision: Int, val scale: Int) extends StaticAnnotation

}
