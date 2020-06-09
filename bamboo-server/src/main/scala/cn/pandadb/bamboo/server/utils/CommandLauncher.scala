package cn.pandadb.bamboo.server.utils

/**
 * Created by bluejoe on 2020/3/25.
 */

// scalastyle:off
trait CommandLauncher {
  val commands: Array[(String, String, ShellCommandExecutor)]
  val launcherName: String

  def main(args: Array[String]): Unit = {
    if (args.length < 1) {
      printError("no command designated")
    }
    else {
      commands.filter(_._3 != null).foreach(x => x._3.init(Array(launcherName, x._1)))

      args(0).toLowerCase() match {
        case "help" =>
          printUsage()
        case cmd: String =>
          val opt = commands.find(_._1.equals(cmd.toLowerCase()))
          if (opt.isDefined) {
            val t1 = System.nanoTime()
            opt.get._3.parseAndRun(args.takeRight(args.length - 1))
            val t2 = System.nanoTime()
            val elapsed = t2 - t1
            if (elapsed > 1000000) {
              println(s"time cost: ${elapsed / 1000000}ms")
            }
            else {
              println(s"time cost: ${elapsed / 1000}us")
            }

          }
          else {
            printError(s"unrecognized command: $cmd")
          }
      }
    }
  }

  private def printError(msg: String): Unit = {
    println(msg)
    printUsage()
  }

  private def printUsage(): Unit = {
    val maxLen = commands.map(_._1.length).max
    println(s"$launcherName <command> [args]")
    println("commands:")
    commands.sortBy(_._1).foreach { en =>
      val space = {
        (1 to (maxLen + 4 - en._1.length)).map(_ => " ").mkString("")
      }
      println(s"\t${en._1}$space${en._2}")
    }
  }
}
// scalastyle:on