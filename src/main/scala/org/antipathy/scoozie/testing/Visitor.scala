package org.antipathy.scoozie.testing

import org.antipathy.scoozie.Node
import scala.collection.immutable.Seq

/**
  * Class for visiting oozie nodes
  *
  * @param visited list of oozie nodes
  * @param failed true if the last node failed
  * @param nextNodeOption the next node to visit
  */
private[testing] case class Visitor(visited: Seq[Seq[String]],
                                    failed: Boolean = false,
                                    nextNodeOption: Option[Node] = None)
