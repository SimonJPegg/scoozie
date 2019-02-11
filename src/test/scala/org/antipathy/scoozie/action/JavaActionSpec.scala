package org.antipathy.scoozie.action

import org.antipathy.scoozie.Scoozie
import org.antipathy.scoozie.configuration.{Configuration, Credentials, Property, YarnConfig}
import org.scalatest.{FlatSpec, Matchers}

import scala.collection.immutable._

class JavaActionSpec extends FlatSpec with Matchers {

  behavior of "JavaAction"

  it should "generate valid XML with no config and no args" in {

    implicit val credentialsOption: Option[Credentials] = None
    val result = JavaAction(name = "SomeAction",
                            mainClass = "org.antipathy.Main",
                            javaJar = "/path/to/jar",
                            javaOptions = "java options",
                            commandLineArgs = Seq(),
                            captureOutput = false,
                            jobXmlOption = None,
                            files = Seq(),
                            prepareOption = None,
                            configuration = Scoozie.Configuration.emptyConfig,
                            yarnConfig = YarnConfig(jobTracker = "jobTracker", nameNode = "nameNode")).action

    scala.xml.Utility.trim(result.toXML) should be(scala.xml.Utility.trim(<java>
        <job-tracker>{"${jobTracker}"}</job-tracker>
        <name-node>{"${nameNode}"}</name-node>
        <main-class>{"${SomeAction_mainClass}"}</main-class>
        <java-opts>{"${SomeAction_javaOptions}"}</java-opts>
        <file>{"${SomeAction_javaJar}"}</file>
      </java>))

    result.properties should be(
      Map("${SomeAction_mainClass}" -> "org.antipathy.Main",
          "${SomeAction_javaJar}" -> "/path/to/jar",
          "${SomeAction_javaOptions}" -> "java options")
    )
  }

  it should "generate valid XML with args" in {

    implicit val credentialsOption: Option[Credentials] = None
    val result = JavaAction(name = "SomeAction",
                            mainClass = "org.antipathy.Main",
                            javaJar = "/path/to/jar",
                            javaOptions = "java options",
                            commandLineArgs = Seq("one", "two"),
                            captureOutput = false,
                            jobXmlOption = None,
                            files = Seq(),
                            prepareOption = None,
                            configuration = Scoozie.Configuration.emptyConfig,
                            yarnConfig = YarnConfig(jobTracker = "jobTracker", nameNode = "nameNode")).action

    scala.xml.Utility.trim(result.toXML) should be(scala.xml.Utility.trim(<java>
        <job-tracker>{"${jobTracker}"}</job-tracker>
        <name-node>{"${nameNode}"}</name-node>
        <main-class>{"${SomeAction_mainClass}"}</main-class>
        <java-opts>{"${SomeAction_javaOptions}"}</java-opts>
        <arg>{"${SomeAction_commandLineArg0}"}</arg>
        <arg>{"${SomeAction_commandLineArg1}"}</arg>
        <file>{"${SomeAction_javaJar}"}</file>
      </java>))

    result.properties should be(
      Map("${SomeAction_commandLineArg0}" -> "one",
          "${SomeAction_javaJar}" -> "/path/to/jar",
          "${SomeAction_javaOptions}" -> "java options",
          "${SomeAction_mainClass}" -> "org.antipathy.Main",
          "${SomeAction_commandLineArg1}" -> "two")
    )
  }

  it should "generate valid XML with config" in {
    implicit val credentialsOption: Option[Credentials] = None
    val result = JavaAction(name = "SomeAction",
                            mainClass = "org.antipathy.Main",
                            javaJar = "/path/to/jar",
                            javaOptions = "java options",
                            commandLineArgs = Seq(),
                            captureOutput = false,
                            jobXmlOption = None,
                            files = Seq(),
                            prepareOption = None,
                            configuration =
                              Configuration(Seq(Property("SomeName1", "SomeVal1"), Property("SomeName2", "SomeVal2"))),
                            yarnConfig = YarnConfig(jobTracker = "jobTracker", nameNode = "nameNode")).action

    scala.xml.Utility.trim(result.toXML) should be(scala.xml.Utility.trim(<java>
        <job-tracker>{"${jobTracker}"}</job-tracker>
        <name-node>{"${nameNode}"}</name-node>
        <configuration>
          <property>
            <name>SomeName1</name>
            <value>{"${SomeAction_property0}"}</value>
          </property>
          <property>
            <name>SomeName2</name>
            <value>{"${SomeAction_property1}"}</value>
          </property>
        </configuration>
        <main-class>{"${SomeAction_mainClass}"}</main-class>
        <java-opts>{"${SomeAction_javaOptions}"}</java-opts>
        <file>{"${SomeAction_javaJar}"}</file>
      </java>))

    result.properties should be(
      Map("${SomeAction_property1}" -> "SomeVal2",
          "${SomeAction_javaJar}" -> "/path/to/jar",
          "${SomeAction_javaOptions}" -> "java options",
          "${SomeAction_mainClass}" -> "org.antipathy.Main",
          "${SomeAction_property0}" -> "SomeVal1")
    )
  }

  it should "generate valid XML with config and parameters" in {
    implicit val credentialsOption: Option[Credentials] = None
    val result = JavaAction(name = "SomeAction",
                            mainClass = "org.antipathy.Main",
                            javaJar = "/path/to/jar",
                            javaOptions = "java options",
                            commandLineArgs = Seq("one", "two"),
                            captureOutput = false,
                            jobXmlOption = None,
                            files = Seq(),
                            prepareOption = None,
                            configuration =
                              Configuration(Seq(Property("SomeName1", "SomeVal1"), Property("SomeName2", "SomeVal2"))),
                            yarnConfig = YarnConfig(jobTracker = "jobTracker", nameNode = "nameNode")).action

    scala.xml.Utility.trim(result.toXML) should be(scala.xml.Utility.trim(<java>
        <job-tracker>{"${jobTracker}"}</job-tracker>
        <name-node>{"${nameNode}"}</name-node>
        <configuration>
          <property>
            <name>SomeName1</name>
            <value>{"${SomeAction_property0}"}</value>
          </property>
          <property>
            <name>SomeName2</name>
            <value>{"${SomeAction_property1}"}</value>
          </property>
        </configuration>
        <main-class>{"${SomeAction_mainClass}"}</main-class>
        <java-opts>{"${SomeAction_javaOptions}"}</java-opts>
        <arg>{"${SomeAction_commandLineArg0}"}</arg>
        <arg>{"${SomeAction_commandLineArg1}"}</arg>
        <file>{"${SomeAction_javaJar}"}</file>
      </java>))

    result.properties should be(
      Map("${SomeAction_property1}" -> "SomeVal2",
          "${SomeAction_commandLineArg0}" -> "one",
          "${SomeAction_javaJar}" -> "/path/to/jar",
          "${SomeAction_javaOptions}" -> "java options",
          "${SomeAction_mainClass}" -> "org.antipathy.Main",
          "${SomeAction_property0}" -> "SomeVal1",
          "${SomeAction_commandLineArg1}" -> "two")
    )
  }

  it should "capture output when required" in {
    implicit val credentialsOption: Option[Credentials] = None
    val result = JavaAction(name = "SomeAction",
                            mainClass = "org.antipathy.Main",
                            javaJar = "/path/to/jar",
                            javaOptions = "java options",
                            commandLineArgs = Seq(),
                            captureOutput = true,
                            jobXmlOption = None,
                            files = Seq(),
                            prepareOption = None,
                            configuration = Scoozie.Configuration.emptyConfig,
                            yarnConfig = YarnConfig(jobTracker = "jobTracker", nameNode = "nameNode")).action

    scala.xml.Utility.trim(result.toXML) should be(scala.xml.Utility.trim(<java>
        <job-tracker>{"${jobTracker}"}</job-tracker>
        <name-node>{"${nameNode}"}</name-node>
        <main-class>{"${SomeAction_mainClass}"}</main-class>
        <java-opts>{"${SomeAction_javaOptions}"}</java-opts>
        <file>{"${SomeAction_javaJar}"}</file>
        <capture-output/>
      </java>))

    result.properties should be(
      Map("${SomeAction_javaJar}" -> "/path/to/jar",
          "${SomeAction_javaOptions}" -> "java options",
          "${SomeAction_mainClass}" -> "org.antipathy.Main")
    )
  }
}
