package enki.arguments

import freestyle.free._

@free trait Arguments {
  def string(name: String, description: String = "", defaultValue: Option[String] = None): FS[String]

  def int(name: String, description: String = "", defaultValue: Option[Int] = None): FS[Int]
}
