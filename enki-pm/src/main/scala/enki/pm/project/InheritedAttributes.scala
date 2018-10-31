package enki.pm.project

import enki.pm.internal._
import cats._
import cats.implicits._
import qq.droste._
import qq.droste.data._

case class InheritedAttributes(
                                qualifiedName: String
                              )

object InheritedAttributes {
  /**
    * Module tree abstracted over fixpoint type A.
    *
    * Module tree is a Rose Tree with modules in leaf nodes where each node associated with derived attributes.
    */
  type ModuleTreeF[A] = AttrRoseTreeF[InheritedAttributes, Module, A]

  /**
    * Carrier type of module coalgebra (product of optional derived attributes and underlying coalgebra carrier A).
    */
  type ModuleCoalgebraCarrier[A] = (A, Option[InheritedAttributes])

  /**
    * Module coalgebra.
    *
    * CoalgebraM on ModuleTreeF with ModuleCoalgebraCarrier abstracted over functor F monad and carrier type A.
    */
  type ModuleCoalgebra[F[_], A] = CoalgebraM[F, ModuleTreeF, ModuleCoalgebraCarrier[A]]

  /**
    * Annotate module coalgebra with inherited attributes.
    *
    * @param moduleName Extract module name from carrier
    * @tparam F Functor type
    * @tparam A Module coalgebra carrier type.
    * @return Module coalgebra.
    */
  def inheritAttributes[F[_] : Applicative, A](
                                                underlyingCoalgebra: Module.Coalgebra[F, A],
                                                moduleName: A => String
                                              ): ModuleCoalgebra[F, A] = CoalgebraM[F, ModuleTreeF, ModuleCoalgebraCarrier[A]] {
    a: ModuleCoalgebraCarrier[A] =>
      val carrier = a._1
      val derivedAttributes = a._2

      def getQualifiedName(moduleName: String, parentModuleNameOpt: Option[String]): String = parentModuleNameOpt match {
        case Some(parentModuleName) => s"$parentModuleName.$moduleName"
        case None => moduleName
      }

      val attributes = InheritedAttributes(
        qualifiedName = getQualifiedName(moduleName(carrier), derivedAttributes.map(_.qualifiedName))
      )

      underlyingCoalgebra.run(carrier).map {
        case Left(module: Module) =>
          AttrF[CoattrF[List, Module, ?], InheritedAttributes, ModuleCoalgebraCarrier[A]](
            attributes,
            CoattrF.pure[List, Module, ModuleCoalgebraCarrier[A]](module))
        case Right(xs: List[A]) =>
          AttrF[CoattrF[List, Module, ?], InheritedAttributes, ModuleCoalgebraCarrier[A]](
            attributes,
            CoattrF.roll[List, Module, ModuleCoalgebraCarrier[A]](xs.map(c => (c, Some(attributes)))))
      }
  }
}