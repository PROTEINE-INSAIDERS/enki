package enki

sealed trait EncoderStyle

object EncoderStyle {

  final object Spark extends EncoderStyle

  final object Enki extends EncoderStyle

}