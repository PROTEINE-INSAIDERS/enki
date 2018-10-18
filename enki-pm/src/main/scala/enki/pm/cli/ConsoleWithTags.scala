package enki.pm.cli

import freestyle.tagless._

@tagless(true) trait ConsoleWithTags {
  def question(s: String): FS[Unit]
}
