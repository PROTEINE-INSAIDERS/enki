package enki.args

import freestyle.free._

@free trait Args {
  def string(name: String, description: String = "", defaultValue: Option[String] = None): FS[String]

  def int(name: String, description: String = "", defaultValue: Option[Int] = None): FS[Int]
}
