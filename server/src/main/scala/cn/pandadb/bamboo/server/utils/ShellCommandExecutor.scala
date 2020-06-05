package cn.pandadb.bamboo.server.utils

import org.apache.commons.cli.{CommandLine, DefaultParser, HelpFormatter, Options, ParseException}

import scala.collection.mutable.ArrayBuffer

// scalastyle:off
trait ShellCommandExecutor {
  val commandNamePath = ArrayBuffer[String]()

  def init(cmds: Array[String]): this.type = {
    commandNamePath ++= cmds
    this
  }

  lazy val OPTIONS: Options = {
    val ops = new Options()
    buildOptions(ops)
    ops
  }

  def buildOptions(options: Options)

  def parseAndRun(args: Array[String]): Unit = {
    val commandLineParser = new DefaultParser()

    try {
      val commandLine = commandLineParser.parse(OPTIONS, args)
      run(commandLine)
    }
    catch {
      case e: ParseException =>
        println(e.getMessage())
        printUsage();
    }
  }

  def run(commandLine: CommandLine)

  private def printUsage(): Unit = {
    val formatter = new HelpFormatter()
    formatter.printHelp(s"${commandNamePath.mkString(" ")}", OPTIONS, true)
    System.out.println()
  }
  // scalastyle:on
}
