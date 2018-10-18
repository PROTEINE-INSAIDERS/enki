package enki.pm.cli

import freestyle.tagless._

@tagless(true) trait Prompt {
  def ask[A](question: Question[A]): FS[A]
}
