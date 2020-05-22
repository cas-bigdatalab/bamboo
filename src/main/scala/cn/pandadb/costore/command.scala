package cn.pandadb.costore

import org.apache.commons.cli._

import scala.collection.mutable.ArrayBuffer

/**
 * Created by bluejoe on 2020/3/25.
 */
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
    val maxlen = commands.map(_._1.length).max
    println(s"$launcherName <command> [args]")
    println("commands:")
    commands.sortBy(_._1).foreach { en =>
      val space = {
        (1 to (maxlen + 4 - en._1.length)).map(_ => " ").mkString("")
      }
      println(s"\t${en._1}$space${en._2}")
    }
  }
}

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
}

object command extends CommandLauncher {
    override val commands: Array[(String, String, ShellCommandExecutor)] = Array[(String, String, ShellCommandExecutor)](
      ("start", "start node server using a conf file", new StartNodeShellCommandExecutor()),
      ("help", "print usage information", null)
  )
  override val launcherName: String = "costore"
}

private class StartNodeShellCommandExecutor extends ShellCommandExecutor {

  override def buildOptions(options: Options): Unit = {
    options.addOption(Option.builder("ipPort")
      .argName("ipPort")
      .desc("Listening address, e.g ip:11234")
      .hasArg
      .required(true)
      .build())

    options.addOption(Option.builder("peers")
      .argName("peers")
      .desc("peers\' Listening address, e.g ip:11234,ip2:11234")
      .hasArg
      .required(true)
      .build())

    options.addOption(Option.builder("replicaFactor")
      .argName("replicaFactor")
      .desc("the number of replica, e.g 3")
      .hasArg
      .required(true)
      .build())
  }

  override def run(commandLine: CommandLine): Unit = {
    val address = commandLine.getOptionValue("ipPort")
    val peers = commandLine.getOptionValue("ipPort").split(',').toList
    val replicaFactor = commandLine.getOptionValue("replicaFactor")
    new NodeService(address, peers, replicaFactor).start()
  }

}