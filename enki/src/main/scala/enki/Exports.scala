package enki

trait Exports extends Aliases {
  type EncoderStyle = enki.EncoderStyle
  val EncoderStyle: enki.EncoderStyle.type = enki.EncoderStyle
  type Environment = enki.Environment
  val Environment: enki.Environment.type = enki.Environment

  type comment = enki.comment
  type decimalPrecision = enki.decimalPrecision
}
