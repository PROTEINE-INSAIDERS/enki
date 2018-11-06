package enki.pm.project

import java.nio.charset.StandardCharsets
import java.nio.file.Path

import cats._
import cats.data.Chain
import cats.implicits._
import enki.pm.fs._
import enki.pm.internal._
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import qq.droste._

sealed trait Module {
  def qualifiedName: Chain[String]
}

// такой способ кодирования не позволяет кодировать ошибку создания самого модуля.
final case class SqlModule(
                            qualifiedName: Chain[String],
                            sql: String,
                            logicalPlan: LogicalPlan
                          ) extends Module

