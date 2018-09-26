package enki
package program

trait Aliases {
  val simpleProgram: ProgramWrapper[Stage.Op] = new ProgramWrapper[Stage.Op]

  type ProgramWrapper[StageAlg[_]] = program.ProgramWrapper[StageAlg]
  type Program1[StageAlg[_], ProgramAlg[_]] = program.ProgramWrapper[StageAlg]#ProgramM[ProgramAlg]
}