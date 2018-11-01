package enki.pm.project

import java.nio.charset.StandardCharsets
import java.nio.file.Path

import cats._
import cats.implicits._
import enki.pm.fs._
import enki.pm.internal._
import qq.droste._

sealed trait Module

final case class SqlModule(sql: String) extends Module

