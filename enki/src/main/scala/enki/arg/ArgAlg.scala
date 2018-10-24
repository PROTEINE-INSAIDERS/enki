package enki.arg

import java.sql.Timestamp

import freestyle.free._

@free trait ArgAlg {
  def bigint(name: String, description: String = "", defaultValue: Option[BigInt]): FS[BigInt]

  def bool(name: String, description: String = "", defaultValue: Option[Boolean]): FS[Boolean]

  def int(name: String, description: String = "", defaultValue: Option[Int] = None): FS[Int]

  def string(name: String, description: String = "", defaultValue: Option[String] = None): FS[String]

  def timestamp(name: String, description: String = "", defaultValue: Option[Timestamp] = None): FS[Timestamp]
}