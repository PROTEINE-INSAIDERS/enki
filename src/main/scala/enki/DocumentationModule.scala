package enki

import scala.annotation.StaticAnnotation

trait DocumentationModule {
  //TODO: сделать систему автодокументирования
  final class comment(val value: String) extends StaticAnnotation
}
