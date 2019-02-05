package org.antipathy.scoozie.action

import org.antipathy.scoozie.action.prepare.Prepare
import org.antipathy.scoozie.configuration.YarnConfig
import org.antipathy.scoozie.configuration.Argument
import scala.xml.Elem
import org.antipathy.scoozie.Node
import org.antipathy.scoozie.configuration.Credentials
import scala.collection.immutable._

/**
  * Oozie Java action definition
  * @param name the name of the action
  * @param script the location of the pig script
  * @param params arguments to the script
  * @param jobXml optional job.xml for the script
  * @param config Yarn configuration for this action
  * @param prepareOption an optional prepare stage for the action
  */
class PigAction(override val name: String,
                script: String,
                params: Seq[String],
                jobXml: Option[String] = None,
                config: YarnConfig,
                prepareOption: Option[Prepare] = None)
    extends Action {

  private val scriptProperty = formatProperty(s"${name}_script")
  private val paramsProperties = buildSequenceProperties(name, "param", params)
  private val jobXmlProperty =
    buildStringOptionProperty(name, "jobXml", jobXml)
  private val mappedConfigAndProperties =
    config.configuration.withActionProperties(name)
  private val mappedConfig = mappedConfigAndProperties._1
  private val prepareOptionAndProps =
    prepareOption.map(_.withActionProperties(name))
  private val prepareProperties =
    prepareOptionAndProps.map(_._2).getOrElse(Map[String, String]())
  private val prepareOptionMapped = prepareOptionAndProps.map(_._1)

  /**
    * Get the Oozie properties for this object
    */
  override def properties: Map[String, String] =
    Map(scriptProperty -> script) ++ config.properties ++
    paramsProperties ++
    jobXmlProperty ++
    prepareProperties ++
    mappedConfigAndProperties._2

  /**
    * The XML namespace for an action element
    */
  override val xmlns: Option[String] = Some("uri:oozie:workflow:0.2")

  /**
    * The XML for this node
    */
  override def toXML: Elem =
    <pig>
      {config.jobTrackerXML}
      {config.nameNodeXML}
      {if (prepareOptionMapped.isDefined) {
          prepareOptionMapped.get.toXML
        }
      }
      {if (jobXml.isDefined) {
          <job-xml>{jobXmlProperty}</job-xml>
        }
      }

      {if (mappedConfig.configProperties.nonEmpty) {
          mappedConfig.toXML
        }
      }
      <script>{scriptProperty}</script>
      {
        val paramSeq = Seq.fill(params.length)("-param")
        paramSeq.zipAll(paramsProperties.keys,"","").flatMap{
            case (a,b) => Seq(a,b) 
        }.map(Argument(_).toXML)
      }
    </pig>
}

object PigAction {

  def apply(name: String,
            script: String,
            params: Seq[String],
            jobXml: Option[String] = None,
            config: YarnConfig,
            prepareOption: Option[Prepare] = None)(implicit credentialsOption: Option[Credentials]): Node =
    Node(new PigAction(name, script, params, jobXml, config, prepareOption))
}
