package org.antipathy.scoozie.testing

import org.antipathy.scoozie.action.control._
import org.antipathy.scoozie.action.{Action, Node}
import org.antipathy.scoozie.exception._
import org.antipathy.scoozie.workflow.Workflow

import scala.collection.immutable._

/**
  * Class for testing Ooize Workflows
  * @param workflow the workflow to test
  * @param failingNodes a list of nodes that should fail in this workflow
  * @param decisionNodes the nodes to visit on a decision
  */
class WorkflowTestRunner(workflow: Workflow, failingNodes: Seq[String], decisionNodes: Seq[String]) {

  /**
    * Traverse the workflow and print a string representation of its path
    */
  def traversalPath: String =
    buildOutputString(visitNode(workflow.transitions, Visitor(visited = Seq.empty)).visited)

  /**
    * Visit the passed in node and its children
    *
    * @param node the node to visit
    * @param visitor the visitor to the node
    * @return the visitor that has visited this node
    */
  private def visitNode(node: Node, visitor: Visitor): Visitor = {

    //Check for loop condition
    if (visitor.visited.nonEmpty && visitor.visited
          .map(_.contains(node.name))
          .reduceLeft(_ || _)) {
      throw new LoopingException(s"Loop detected on action: ${node.name}")
    }

    val thisVisitor = visitor.copy(visited = visitor.visited :+ Seq(node.name))
    val isFailingNode = failingNodes.contains(node.name)

    (isFailingNode, node.successTransition, node.failureTransition) match {
      case (false, Some(successNode), _) => visitNode(successNode, thisVisitor)
      case (true, _, Some(failureNode))  => visitNode(failureNode, thisVisitor)
      case _                             => visitAction(node.action, thisVisitor)
    }
  }

  /**
    * Decide the next node to visit based on the action calling it
    * @param action the action to transition from
    * @param visitor the visitor to the action
    * @return the visitor that has visited the subsequent nodes
    */
  private def visitAction(action: Action, visitor: Visitor): Visitor =
    action match {
      case f: Fork     => visitFork(f, visitor)
      case j: Join     => visitNode(j.transitionTo, visitor)
      case d: Decision => visitDecision(d, visitor)
      case _           => visitor

    }

  /**
    * Visit the passed in decision node and determine the next node in the sequence
    *
    * @param decision The decision to visit
    * @param visitor the visitor to the decision
    * @return the visitor that has visited this node
    */
  def visitDecision(decision: Decision, visitor: Visitor): Visitor =
    decision.transitionPaths.filter(n => decisionNodes.contains(n.name)) match {
      case Nil         => visitNode(decision.defaultPath, visitor)
      case head :: Nil => visitNode(head, visitor)
      case _           => throw new TransitionException(s"multiple paths specified for node: ${decision.name}")
    }

  /**
    * Navigate a fork node
    *
    * <B>Forks containing forks or decisions currently unsupported.</B>
    *
    * @param fork The fork to traverse
    * @param visitor the visitor to the fork
    * @return the visitor that has visited this fork
    */
  def visitFork(fork: Fork, visitor: Visitor): Visitor = {
    val childNodes = Seq(fork.transitionPaths: _*)
    val simpleForkVisitor = visitor.copy(visited = visitor.visited ++ Seq(childNodes.map(n => n.action.name)))
    //failures in initial path
    val hasSimpleFailures = childNodes.map(_.action.name).exists(failingNodes.contains(_))
    val isSimpleFork = childNodes
      .flatMap(_.successTransition)
      .map(_.action)
      .map {
        case _: Join => 1
        case _       => 0
      }
      .sum == childNodes.length

    (hasSimpleFailures, isSimpleFork) match {
      case (true, _) => visitNode(childNodes.flatMap(_.failureTransition).head, simpleForkVisitor)
      case (_, true) => visitNode(childNodes.flatMap(_.successTransition).head, simpleForkVisitor)
      case _ =>
        val outComes = for {
          childNode <- childNodes
          visitation = visitUntilJoin(childNode, Visitor(Seq()))
          text = visitation.visited.flatten
          node <- visitation.nextNodeOption
          failed = visitation.failed
        } yield (failed, node, text)

        val outputString = Seq(Seq(s"(${outComes.map { l =>
          l._3.mkString(" -> ")
        }.mkString(", ")})"))

        val thisVisitor = visitor.copy(visited = visitor.visited ++ outputString)
        if (outComes.map(_._1).foldLeft(false)(_ || _)) {
          visitNode(outComes.filter(_._1 == true).map(_._2).head, thisVisitor)
        } else {
          visitNode(outComes.map(_._2).head, thisVisitor)
        }
    }
  }

  /**
    * Visit the passed in node and its children until a Join node is reached
    * @param node the node to visit
    * @param visitor the visitor to the node
    * @return the visitor that has visited this node
    */
  private def visitUntilJoin(node: Node, visitor: Visitor): Visitor = {
    val thisVisitor = visitor.copy(visited = visitor.visited :+ Seq(node.name))
    val isFailingNode = failingNodes.contains(node.name)

    (isFailingNode, node.successTransition, node.failureTransition) match {
      case (false, Some(successNode), _) =>
        visitUntilJoin(successNode, Visitor(thisVisitor.visited, isFailingNode, Some(successNode)))
      case (true, _, Some(failureNode)) =>
        Visitor(thisVisitor.visited, isFailingNode, Some(failureNode))
      case _ => visitor
    }

  }

  /**
    * format the output string
    */
  private def buildOutputString(transitions: Seq[Seq[String]]): String = {
    val x = transitions.map {
      case head :: Nil => head
      case list        => s"(${list.mkString(", ")})"
    }.mkString(" -> ")

    x
  }

}

object WorkflowTestRunner {

  def apply(workflow: Workflow,
            failingNodes: Seq[String] = Seq.empty[String],
            decisionNodes: Seq[String] = Seq.empty[String]): WorkflowTestRunner =
    new WorkflowTestRunner(workflow, failingNodes, decisionNodes)
}
