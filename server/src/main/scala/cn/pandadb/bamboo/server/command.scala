package cn.pandadb.bamboo.server

import cn.pandadb.bamboo.server.utils.{CommandLauncher, ShellCommandExecutor}
import org.apache.commons.cli.{CommandLine, Option, Options}

object command extends CommandLauncher {
    override val commands: Array[(String, String, ShellCommandExecutor)] = Array[(String, String, ShellCommandExecutor)](
      ("start", "start node server using a conf file", new StartNodeShellCommandExecutor()),
      ("help", "print usage information", null)
  )
  override val launcherName: String = "bamboo"
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
    val peers = commandLine.getOptionValue("peers").split(',').toList
    val replicaFactor = commandLine.getOptionValue("replicaFactor").toInt
    new NodeService(address, peers, replicaFactor).start()
  }

}