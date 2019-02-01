package org.antipathy.scoozie.action

import org.scalatest.{FlatSpec, Matchers}
import org.antipathy.scoozie.configuration.{
  Configuration,
  Property,
  YarnConfig
}
import org.antipathy.scoozie.configuration.Credentials
import scala.xml
import scala.collection.immutable._

class SparkActionSpec extends FlatSpec with Matchers {

  behavior of "SparkAction"

  it should "generate valid XML with no config and no args" in {

    implicit val credentialsOption: Option[Credentials] = None

    val result = SparkAction(name = "SomeAction",
                             sparkSettings = "/path/to/spark/settings",
                             sparkMasterURL = "masterURL",
                             sparkMode = "mode",
                             sparkJobName = "JobName",
                             mainClass = "org.antipathy.Main",
                             sparkJar = "/path/to/jar",
                             sparkOptions = "spark options",
                             commandLineArgs = Seq(),
                             files = Seq(),
                             prepareOption = None,
                             config = YarnConfig(jobTracker = "jobTracker",
                                                 nameNode = "nameNode")).action

    xml.Utility.trim(result.toXML) should be(
      xml.Utility.trim(<spark xmlns="uri:oozie:spark-action:1.0">
          <job-tracker>{"${jobTracker}"}</job-tracker>
          <name-node>{"${nameNode}"}</name-node>
          <job-xml>{"${SomeAction_sparkSettings}"}</job-xml>
          <master>{"${SomeAction_sparkMasterURL}"}</master>
          <mode>{"${SomeAction_sparkMode}"}</mode>
          <name>{"${SomeAction_sparkJobName}"}</name>
          <class>{"${SomeAction_mainClass}"}</class>
          <jar>{"${SomeAction_sparkJar}"}</jar>
          <spark-opts>{"${SomeAction_sparkOptions}"}</spark-opts>
        </spark>)
    )

    result.properties should be(
      Map("${nameNode}" -> "nameNode",
          "${SomeAction_sparkMode}" -> "mode",
          "${jobTracker}" -> "jobTracker",
          "${SomeAction_sparkMasterURL}" -> "masterURL",
          "${SomeAction_sparkJobName}" -> "JobName",
          "${SomeAction_mainClass}" -> "org.antipathy.Main",
          "${SomeAction_sparkOptions}" -> "spark options",
          "${SomeAction_sparkSettings}" -> "/path/to/spark/settings",
          "${SomeAction_sparkJar}" -> "/path/to/jar")
    )
  }

  it should "generate valid XML with args" in {

    implicit val credentialsOption: Option[Credentials] = None
    val result = SparkAction(name = "SomeAction",
                             sparkSettings = "/path/to/spark/settings",
                             sparkMasterURL = "masterURL",
                             sparkMode = "mode",
                             sparkJobName = "JobName",
                             mainClass = "org.antipathy.Main",
                             sparkJar = "/path/to/jar",
                             sparkOptions = "spark options",
                             commandLineArgs = Seq("one", "two", "three"),
                             files = Seq(),
                             prepareOption = None,
                             config = YarnConfig(jobTracker = "jobTracker",
                                                 nameNode = "nameNode")).action

    xml.Utility.trim(result.toXML) should be(
      xml.Utility.trim(<spark xmlns="uri:oozie:spark-action:1.0">
          <job-tracker>{"${jobTracker}"}</job-tracker>
          <name-node>{"${nameNode}"}</name-node>
          <job-xml>{"${SomeAction_sparkSettings}"}</job-xml>
          <master>{"${SomeAction_sparkMasterURL}"}</master>
          <mode>{"${SomeAction_sparkMode}"}</mode>
          <name>{"${SomeAction_sparkJobName}"}</name>
          <class>{"${SomeAction_mainClass}"}</class>
          <jar>{"${SomeAction_sparkJar}"}</jar>
          <spark-opts>{"${SomeAction_sparkOptions}"}</spark-opts>
          <arg>{"${SomeAction_commandLineArg0}"}</arg>
          <arg>{"${SomeAction_commandLineArg1}"}</arg>
          <arg>{"${SomeAction_commandLineArg2}"}</arg>
        </spark>)
    )

    result.properties should be(
      Map("${nameNode}" -> "nameNode",
          "${SomeAction_sparkMode}" -> "mode",
          "${jobTracker}" -> "jobTracker",
          "${SomeAction_commandLineArg0}" -> "one",
          "${SomeAction_sparkMasterURL}" -> "masterURL",
          "${SomeAction_commandLineArg2}" -> "three",
          "${SomeAction_sparkJobName}" -> "JobName",
          "${SomeAction_mainClass}" -> "org.antipathy.Main",
          "${SomeAction_sparkOptions}" -> "spark options",
          "${SomeAction_sparkSettings}" -> "/path/to/spark/settings",
          "${SomeAction_commandLineArg1}" -> "two",
          "${SomeAction_sparkJar}" -> "/path/to/jar")
    )
  }

  it should "generate valid XML with config" in {

    implicit val credentialsOption: Option[Credentials] = None
    val result = SparkAction(name = "SomeAction",
                             sparkSettings = "/path/to/spark/settings",
                             sparkMasterURL = "masterURL",
                             sparkMode = "mode",
                             sparkJobName = "JobName",
                             mainClass = "org.antipathy.Main",
                             sparkJar = "/path/to/jar",
                             sparkOptions = "spark options",
                             commandLineArgs = Seq(),
                             files = Seq(),
                             config = YarnConfig(jobTracker = "jobTracker",
                                                 nameNode = "nameNode",
                                                 configuration = Configuration(
                                                   Seq(Property(name =
                                                                  "SomeProp1",
                                                                "SomeValue1"),
                                                       Property(name =
                                                                  "SomeProp2",
                                                                "SomeValue2"))
                                                 ))).action

    xml.Utility.trim(result.toXML) should be(
      xml.Utility.trim(<spark xmlns="uri:oozie:spark-action:1.0">
          <job-tracker>{"${jobTracker}"}</job-tracker>
          <name-node>{"${nameNode}"}</name-node>
          <job-xml>{"${SomeAction_sparkSettings}"}</job-xml>
          <configuration>
            <property><name>SomeProp1</name>
              <value>{"${SomeAction_property0}"}</value>
            </property>
            <property>
              <name>SomeProp2</name>
              <value>{"${SomeAction_property1}"}</value>
            </property>
          </configuration>
          <master>{"${SomeAction_sparkMasterURL}"}</master>
          <mode>{"${SomeAction_sparkMode}"}</mode>
          <name>{"${SomeAction_sparkJobName}"}</name>
          <class>{"${SomeAction_mainClass}"}</class>
          <jar>{"${SomeAction_sparkJar}"}</jar>
          <spark-opts>{"${SomeAction_sparkOptions}"}</spark-opts>
        </spark>)
    )

    result.properties should be(
      Map("${SomeAction_property1}" -> "SomeValue2",
          "${nameNode}" -> "nameNode",
          "${SomeAction_sparkMode}" -> "mode",
          "${jobTracker}" -> "jobTracker",
          "${SomeAction_sparkMasterURL}" -> "masterURL",
          "${SomeAction_sparkJobName}" -> "JobName",
          "${SomeAction_mainClass}" -> "org.antipathy.Main",
          "${SomeAction_sparkOptions}" -> "spark options",
          "${SomeAction_sparkSettings}" -> "/path/to/spark/settings",
          "${SomeAction_property0}" -> "SomeValue1",
          "${SomeAction_sparkJar}" -> "/path/to/jar")
    )
  }

  it should "generate valid XML with config and parameters" in {

    implicit val credentialsOption: Option[Credentials] = None
    val result = SparkAction(name = "SomeAction",
                             sparkSettings = "/path/to/spark/settings",
                             sparkMasterURL = "masterURL",
                             sparkMode = "mode",
                             sparkJobName = "JobName",
                             mainClass = "org.antipathy.Main",
                             sparkJar = "/path/to/jar",
                             sparkOptions = "spark options",
                             commandLineArgs = Seq("one", "two", "three"),
                             files = Seq(),
                             prepareOption = None,
                             config = YarnConfig(jobTracker = "jobTracker",
                                                 nameNode = "nameNode",
                                                 configuration = Configuration(
                                                   Seq(Property(name =
                                                                  "SomeProp1",
                                                                "SomeValue1"),
                                                       Property(name =
                                                                  "SomeProp2",
                                                                "SomeValue2"))
                                                 ))).action

    xml.Utility.trim(result.toXML) should be(
      xml.Utility.trim(<spark xmlns="uri:oozie:spark-action:1.0">
          <job-tracker>{"${jobTracker}"}</job-tracker>
          <name-node>{"${nameNode}"}</name-node>
          <job-xml>{"${SomeAction_sparkSettings}"}</job-xml>
          <configuration>
            <property>
              <name>SomeProp1</name>
              <value>{"${SomeAction_property0}"}</value>
            </property>
            <property>
              <name>SomeProp2</name>
              <value>{"${SomeAction_property1}"}</value>
            </property>
          </configuration>
          <master>{"${SomeAction_sparkMasterURL}"}</master>
          <mode>{"${SomeAction_sparkMode}"}</mode>
          <name>{"${SomeAction_sparkJobName}"}</name>
          <class>{"${SomeAction_mainClass}"}</class>
          <jar>{"${SomeAction_sparkJar}"}</jar>
          <spark-opts>{"${SomeAction_sparkOptions}"}</spark-opts>
          <arg>{"${SomeAction_commandLineArg0}"}</arg>
          <arg>{"${SomeAction_commandLineArg1}"}</arg>
          <arg>{"${SomeAction_commandLineArg2}"}</arg></spark>)
    )

    result.properties should be(
      Map("${SomeAction_property1}" -> "SomeValue2",
          "${nameNode}" -> "nameNode",
          "${SomeAction_sparkMode}" -> "mode",
          "${jobTracker}" -> "jobTracker",
          "${SomeAction_commandLineArg0}" -> "one",
          "${SomeAction_sparkMasterURL}" -> "masterURL",
          "${SomeAction_commandLineArg2}" -> "three",
          "${SomeAction_sparkJobName}" -> "JobName",
          "${SomeAction_mainClass}" -> "org.antipathy.Main",
          "${SomeAction_sparkOptions}" -> "spark options",
          "${SomeAction_sparkSettings}" -> "/path/to/spark/settings",
          "${SomeAction_property0}" -> "SomeValue1",
          "${SomeAction_commandLineArg1}" -> "two",
          "${SomeAction_sparkJar}" -> "/path/to/jar")
    )
  }
}
