package org.antipathy.scoozie.builder

import org.antipathy.scoozie.action._
import org.antipathy.scoozie.action.control._
import com.typesafe.config.Config
import org.antipathy.scoozie.exception.UnknownActionException
import scala.collection.immutable.Seq
import org.antipathy.scoozie.configuration.Credentials
import org.antipathy.scoozie.configuration.YarnConfig
import scala.util.control.NonFatal
import scala.collection.JavaConverters._
import com.typesafe.config.ConfigException
import org.antipathy.scoozie.exception._

/**
  * Object to convert Hocon action definitions into Action objects
  */
private[scoozie] object TransitionBuilder {

  /**
    * Build a sequence of actions from the passed in config items
    *
    * @param configSeq the collection of configuration items to build from
    * @param yarnConfig a yarn configuration for the actions
    * @param credentials a set of credentials for the actions
    * @return a collection of Action objects
    */
  def build(configSeq: Seq[_ <: Config], yarnConfig: YarnConfig)(implicit credentials: Option[Credentials]): Node = {
    val actions = Seq(configSeq.map { config =>
      getType(config) match {
        case "start"      => NodeWithConfig(Start(), config)
        case "distcp"     => NodeWithConfig(DistCPAction(config, yarnConfig), config)
        case "email"      => NodeWithConfig(EmailAction(config), config)
        case "filesystem" => NodeWithConfig(FsAction(config), config)
        case "hive"       => NodeWithConfig(HiveAction(config, yarnConfig), config)
        case "java"       => NodeWithConfig(JavaAction(config, yarnConfig), config)
        case "pig"        => NodeWithConfig(PigAction(config, yarnConfig), config)
        case "shell"      => NodeWithConfig(ShellAction(config, yarnConfig), config)
        case "spark"      => NodeWithConfig(SparkAction(config, yarnConfig), config)
        case "sqoop"      => NodeWithConfig(SqoopAction(config, yarnConfig), config)
        case "ssh"        => NodeWithConfig(SshAction(config), config)
        case "subworflow" => NodeWithConfig(SubWorkflowAction(config, yarnConfig), config)
        case "end"        => NodeWithConfig(End(), config)
        case "kill"       => NodeWithConfig(Kill(config.getString("message")), config)
        //build placeholders for fork,join and decision nodes
        case "fork"     => NodeWithConfig(Fork(config.getString("name"), Seq.empty), config)
        case "join"     => NodeWithConfig(Join(config.getString("name"), End()), config)
        case "decision" => NodeWithConfig(Decision(config.getString("name"), End(), Seq.empty), config)
        case unknown =>
          throw new UnknownActionException(s"action type $unknown is invalid")
      }
    }: _*)

    val startNode = try {
      actions.filter(_.node.action.name.equalsIgnoreCase("start")).head
    } catch {
      case _: NoSuchElementException =>
        throw new ConfigurationMissingException(s"Could not find start action")
    }

    setTransitions(startNode, actions)
  }

  /**
    *  Walk the transitions specified in the config and build the Oozie DAG
    */
  private def setTransitions(nodeWithConfig: NodeWithConfig, nodes: Seq[NodeWithConfig]): Node = {
    val currentNode = nodeWithConfig.node
    val currentConfig = nodeWithConfig.config

    currentConfig.getString("type") match {
      case "start"        => setTransition(currentNode, currentConfig, nodes, "ok-to", currentNode.okTo)
      case "fork"         => buildFork(currentNode, currentConfig, nodes)
      case "join"         => buildJoin(nodes, currentNode, currentConfig)
      case "decision"     => buildDecision(nodes, currentNode, currentConfig)
      case "kill" | "end" => currentNode
      case _ =>
        val nodeWithOk = setTransition(currentNode, currentConfig, nodes, "ok-to", currentNode.okTo)
        val nodeWithError = setTransition(nodeWithOk, currentConfig, nodes, "error-to", nodeWithOk.errorTo)
        nodeWithError
    }
  }

  /**
    *  (re)build a decision node with the correct transitions.
    */
  private def buildDecision(nodes: Seq[NodeWithConfig], currentNode: Node, currentConfig: Config) = {
    val decisionName = currentNode.action.name
    val defaultName = try {
      currentConfig.getString("default")
    } catch {
      case NonFatal(_) =>
        throw new ConfigurationMissingException(s"No default specified for decision '$decisionName'")
    }
    val defaultNode = try {
      nodes.filter(nodeWithConfig => nodeWithConfig.node.action.name.equalsIgnoreCase(defaultName)).head
    } catch {
      case NonFatal(_) =>
        throw new TransitionException(s"could not find default node '$defaultName' for decision '$decisionName'")
    }
    val defaultNodeWithTransitions = setTransitions(defaultNode, nodes)
    val switchesConfig = currentConfig.getConfig("switches")
    val switches = switchesConfig
      .entrySet()
      .asScala
      .toSeq
      .sortBy(_.getKey)
      .map { item =>
        val switchCase = item.getValue.render().replace("\"", "")
        val pathNode = try {
          nodes.filter(n => item.getKey.equalsIgnoreCase(n.node.action.name)).head
        } catch {
          case NonFatal(_) =>
            throw new TransitionException(s"could not find switch node '$defaultName' for decision '$decisionName'")
        }
        val switchNode = setTransitions(pathNode, nodes)
        Switch(switchNode, switchCase)
      }
      .toSeq
    Decision(decisionName, defaultNodeWithTransitions, Seq(switches: _*))
  }

  /**
    *  (re)build a join node with the correct transitions.
    */
  private def buildJoin(nodes: Seq[NodeWithConfig], currentNode: Node, currentConfig: Config): Node = {
    val joinName = currentNode.action.name
    val toName = currentConfig.getString("ok-to")
    val toNode = try {
      nodes.filter(nodeWithConfig => nodeWithConfig.node.action.name.equalsIgnoreCase(toName)).head
    } catch {
      case NonFatal(_) =>
        throw new TransitionException(s"could not find next node '$toName' for join '$joinName'")
    }
    val newJoin = Join(joinName, setTransitions(toNode, nodes))
    setTransition(newJoin, currentConfig, nodes, "ok-to", newJoin.okTo)
  }

  /**
    *  (re)build a fork node with the correct transitions.
    */
  private def buildFork(currentNode: Node, currentConfig: Config, nodes: Seq[NodeWithConfig]): Node = {
    val forkName = currentNode.action.name
    val pathNames = currentConfig.getStringList("paths")

    if (pathNames.size() < 2) {
      throw new TransitionException("Fork 'forkName' has less than two transitions")
    }
    val paths = nodes.filter(nodeWithConfig => pathNames.contains(nodeWithConfig.node.action.name))
    Fork(forkName, paths.map(n => setTransitions(n, nodes)))
  }

  /**
    * Set the transition for the passed in node
    *
    * @param currentNode the node to set the tranistion for
    * @param currentConfig the configuration for the node
    * @param nodes the nodes in the transition
    * @param transitionType the type of transition for perform
    * @param transitionFunction the function to set the transistion
    * @return a node with the specified transistion set
    */
  private def setTransition(currentNode: Node,
                            currentConfig: Config,
                            nodes: Seq[NodeWithConfig],
                            transitionType: String,
                            transitionFunction: Node => Node): Node = {
    val nextNodeName = try {
      currentConfig.getString(transitionType)
    } catch {
      case c: ConfigException =>
        throw new ConfigurationMissingException(s"${c.getMessage} in ${currentConfig.getString("name")}")
    }

    val nextNodeWithConfig = try {
      nodes.filter(_.node.action.name.equalsIgnoreCase(nextNodeName)).head
    } catch {
      case _: NoSuchElementException =>
        throw new ConfigurationMissingException(s"Could not find node '${currentConfig
          .getString(transitionType)}' when setting transition for ${currentConfig.getString("name")}")
    }
    val nextNode = setTransitions(nextNodeWithConfig, nodes)
    transitionFunction(nextNode)
  }

  /**
    * get the type of action being built
    *
    * @param config the config for the object
    * @return the action type
    */
  private def getType(config: Config): String =
    try {
      config.getString("type").toLowerCase
    } catch {
      case NonFatal(_) =>
        throw new UnknownActionException(s"no action type specified for ${config.getString("name")}")
    }
}
