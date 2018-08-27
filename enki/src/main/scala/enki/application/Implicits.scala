package enki.application

import com.monovore.decline._

trait Implicits {
  implicit def enkiMainToOpts(enkiMain: EnkiMain): Opts[Unit] = enkiMain.main
}
