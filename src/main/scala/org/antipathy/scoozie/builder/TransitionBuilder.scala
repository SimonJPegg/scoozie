package org.antipathy.scoozie.builder

import com.typesafe.config.Config
import org.antipathy.scoozie.action._
import org.antipathy.scoozie.action.control._
import org.antipathy.scoozie.configuration.{Credentials, YarnConfig}
import org.antipathy.scoozie.exception.{UnknownActionException, _}

import scala.collection.JavaConverters._
import scala.collection.immutable.Seq
import scala.util._

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
    val actions = Seq(configSeq.map(c => (getType(c), c)).map {
      case (HoconConstants.start, config)      => NodeWithConfig(Start(), config)
      case (HoconConstants.distcp, config)     => NodeWithConfig(DistCPAction(config, yarnConfig), config)
      case (HoconConstants.email, config)      => NodeWithConfig(EmailAction(config), config)
      case (HoconConstants.filesystem, config) => NodeWithConfig(FsAction(config), config)
      case (HoconConstants.hive, config)       => NodeWithConfig(HiveAction(config, yarnConfig), config)
      case (HoconConstants.java, config)       => NodeWithConfig(JavaAction(config, yarnConfig), config)
      case (HoconConstants.pig, config)        => NodeWithConfig(PigAction(config, yarnConfig), config)
      case (HoconConstants.shell, config)      => NodeWithConfig(ShellAction(config, yarnConfig), config)
      case (HoconConstants.spark, config)      => NodeWithConfig(SparkAction(config, yarnConfig), config)
      case (HoconConstants.sqoop, config)      => NodeWithConfig(SqoopAction(config, yarnConfig), config)
      case (HoconConstants.ssh, config)        => NodeWithConfig(SshAction(config), config)
      case (HoconConstants.subworflow, config) => NodeWithConfig(SubWorkflowAction(config, yarnConfig), config)
      case (HoconConstants.end, config)        => NodeWithConfig(End(), config)
      case (HoconConstants.kill, config)       => NodeWithConfig(Kill(config.getString(HoconConstants.message)), config)
      //build placeholders for fork,join and decision nodes
      case (HoconConstants.fork, config) =>
        NodeWithConfig(Fork(config.getString(HoconConstants.name), Seq.empty), config)
      case (HoconConstants.join, config) => NodeWithConfig(Join(config.getString(HoconConstants.name), End()), config)
      case (HoconConstants.decision, config) =>
        NodeWithConfig(Decision(config.getString(HoconConstants.name), End(), Seq.empty), config)
      case (unknown, _) =>
        throw new UnknownActionException(s"action type $unknown is invalid")
    }: _*)

    val startNode = Try {
      actions.filter(_.node.action.name.equalsIgnoreCase(HoconConstants.start)).head
    } match {
      case Success(value) => value
      case Failure(_) =>
        throw new ConfigurationMissingException(s"Could not find ${HoconConstants.start} action")
    }

    setTransitions(startNode, actions)
  }

  /**
    *  Walk the transitions specified in the config and build the Oozie DAG
    */
  private def setTransitions(nodeWithConfig: NodeWithConfig, nodes: Seq[NodeWithConfig]): Node = {
    val currentNode = nodeWithConfig.node
    val currentConfig = nodeWithConfig.config

    currentConfig.getString(HoconConstants.typ) match {
      case HoconConstants.start =>
        setTransition(currentNode, currentConfig, nodes, HoconConstants.okTo, currentNode.okTo)
      case HoconConstants.fork                      => buildFork(currentNode, currentConfig, nodes)
      case HoconConstants.join                      => buildJoin(nodes, currentNode, currentConfig)
      case HoconConstants.decision                  => buildDecision(nodes, currentNode, currentConfig)
      case HoconConstants.kill | HoconConstants.end => currentNode
      case _ =>
        val nodeWithOk = setTransition(currentNode, currentConfig, nodes, HoconConstants.okTo, currentNode.okTo)
        val nodeWithError = setTransition(nodeWithOk, currentConfig, nodes, HoconConstants.errorTo, nodeWithOk.errorTo)
        nodeWithError
    }
  }

  /**
    *  (re)build a decision node with the correct transitions.
    */
  private def buildDecision(nodes: Seq[NodeWithConfig], currentNode: Node, currentConfig: Config) = {
    val decisionName = currentNode.action.name
    val defaultName = Try {
      currentConfig.getString(HoconConstants.default)
    } match {
      case Success(value) => value
      case Failure(_) =>
        throw new ConfigurationMissingException(s"No default specified for decision '$decisionName'")
    }
    val defaultNode = Try {
      nodes.filter(nodeWithConfig => nodeWithConfig.node.action.name.equalsIgnoreCase(defaultName)).head
    } match {
      case Success(value) => value
      case Failure(_) =>
        throw new TransitionException(s"could not find default node '$defaultName' for decision '$decisionName'")
    }
    val defaultNodeWithTransitions = setTransitions(defaultNode, nodes)
    val switchesConfig = currentConfig.getConfig(HoconConstants.switches)
    val switches = switchesConfig
      .entrySet()
      .asScala
      .toSeq
      .sortBy(_.getKey)
      .map { item =>
        val switchCase = item.getValue.render().replace("\"", "")
        val pathNode = Try {
          nodes.filter(n => item.getKey.equalsIgnoreCase(n.node.action.name)).head
        } match {
          case Success(value) => value
          case Failure(_) =>
            throw new TransitionException(s"could not find switch node '$defaultName' for decision '$decisionName'")
        }
        val switchNode = setTransitions(pathNode, nodes)
        Switch(switchNode, switchCase)
      }
    Decision(decisionName, defaultNodeWithTransitions, Seq(switches: _*))
  }

  /**
    *  (re)build a join node with the correct transitions.
    */
  private def buildJoin(nodes: Seq[NodeWithConfig], currentNode: Node, currentConfig: Config): Node = {
    val joinName = currentNode.action.name
    val toName = currentConfig.getString(HoconConstants.okTo)
    val toNode =
      nodes.find(nodeWithConfig => nodeWithConfig.node.action.name.equalsIgnoreCase(toName)) match {
        case Some(value) => value
        case None =>
          throw new TransitionException(s"could not find next node '$toName' for join '$joinName'")
      }
    val newJoin = Join(joinName, setTransitions(toNode, nodes))
    setTransition(newJoin, currentConfig, nodes, HoconConstants.okTo, newJoin.okTo)
  }

  /**
    *  (re)build a fork node with the correct transitions.
    */
  private def buildFork(currentNode: Node, currentConfig: Config, nodes: Seq[NodeWithConfig]): Node = {
    val forkName = currentNode.action.name
    val pathNames = currentConfig.getStringList(HoconConstants.paths)

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
    val nextNodeName = Try {
      currentConfig.getString(transitionType)
    } match {
      case Success(value) => value
      case Failure(exception) =>
        throw new ConfigurationMissingException(
          s"${exception.getMessage} in ${currentConfig.getString(HoconConstants.name)}"
        )
    }

    val nextNodeWithConfig =
      nodes.find(_.node.action.name.equalsIgnoreCase(nextNodeName)) match {
        case Some(value) => value
        case None =>
          throw new ConfigurationMissingException(s"Could not find node '${currentConfig
            .getString(transitionType)}' when setting transition for ${currentConfig.getString(HoconConstants.name)}")
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
    Try {
      config.getString(HoconConstants.typ).toLowerCase
    } match {
      case Success(value) => value
      case Failure(_) =>
        throw new UnknownActionException(s"no action type specified for ${config.getString(HoconConstants.name)}")
    }
}
