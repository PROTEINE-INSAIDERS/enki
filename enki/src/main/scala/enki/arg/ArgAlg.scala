package enki.arg

import freestyle.free._

@free trait ArgAlg {
  def bool(name: String, description: String = "", defaultValue: Option[Boolean]): FS[Boolean]

  def int(name: String, description: String = "", defaultValue: Option[Int] = None): FS[Int]

  def string(name: String, description: String = "", defaultValue: Option[String] = None): FS[String]
}