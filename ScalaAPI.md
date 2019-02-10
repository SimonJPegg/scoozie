# Scala API 

## Usage

Scoozie provides all its functionality via the `Scoozie` object and the following sub-objects are exposed:

* `Actions`: Methods for creating the following Oozie actions are provided:
	* DistCP
	* Email
	* FileSystem
	* Hive2
	* Hive
	* Java
	* Pig
	* Shell
	* Spark
	* Sqoop
	* Ssh
	* Sub-Workflow
	* Oozie control nodes (Start, End, Fork, Kill, Decision)


* `FileSystem`: Provides methods for creating file system operations for FileSystem actions and an action's prepare steps:
	* chmod
	* delete
	* mkdir
	* move
	* touchz

* `Prepare`: Provides methods for creating prepare steps for actions

*  `Configuration`: Provides methods for creating credentials, properties and other configuration.

* `Test`: Provides methods for testing and validating workflows

* `Functions`: Provides the following three sub functions:
	* `WorkFlow`: Common oozie workflow functions such as getting the workflow id or the last error message
	* `Basic`: Oozie string and time EL functions
	* `Coordinator`: Oozie coordinator time functions (datasets are not currently supported)

In addition workflows and coordinators may be generated via the `Scoozie.workflow` and `Scoozie.coordinator` methods.

### Defining transitions between actions

Each generated action exposes an `okTo` and an `errorTo` function that take the next action as an argument.  Transitions should begin with the `Start` action and end with the `End` or `Kill` action.  

### Authentication
Each action expects an implicit `Option[Credentials]` object for specifying credentials for a workflow.  These can be created via the following methods:

For jobs without credentials:

```scala
implicit val credentials: Option[Credentials] = Scoozie.Configuration.emptyCredentials
```
For jobs with credentials:

```scala
  implicit val credentials: Option[Credentials] = Scoozie.Configuration.credentials("hive-credentials","hive", Seq(Property(name = "name", value = "value"))
```


### Testing
Scoozie provides validation of the generated XML against the Oozie XSDs for each of the actions listed above as well as the workflow and coordinators (via the `Scoozie.Test.validate(x)` methods).  
In addition basic loop checking and testing of workflow transitions can be achieved via the `WorkflowTestRunner ` class.

For example:

```scala
val transitions = {
      val spark = sparkAction okTo End() errorTo kill
      val hive3 = hiveAction3 okTo End() errorTo kill
      val hive2 = hiveAction2 okTo End() errorTo kill
      val decision = Decision("decisionNode", spark, Switch(hive2, "${someVar}"), Switch(hive3, "${someOtherVar}"))
      val hive = hiveAction okTo decision errorTo kill
      Start() okTo hive
    }

    val workflow = Workflow(name = "sampleWorkflow",
                            path = "", //HDFS path
                            transitions = transitions,
                            configuration =
                              Configuration(Seq(Property(name = "workflowprop", value = "workflowpropvalue"))),
                            yarnConfig = yarnConfig)

    val workflowTestRunner = WorkflowTestRunner(workflow, Seq(hiveAction3.name))

    workflowTestRunner.traversalPath should be("start -> hiveAction -> decisionNode -> sparkAction -> end")
```

The transitions are represented as a string with the ` -> ` symbol representing a transition between two nodes.  No distinction is made between a successful transition or an error transition for this representation.  Forks are represented as 

```scala
Start -> someFork -> (action1, action2) -> someJoin -> End
```

or on a failed node in a fork

```scala
Start -> someFork -> (action1, action2) -> kill
```

Further examples can be seen in the `WorkflowTestRunnerSpec` class. This class takes constructor arguments to specify the names of actions to fail, or to use on decision points.

### Exposed traits 
The `ScoozieCoordinator` and `ScoozieWorkflow` traits are exposed for clients of this library to implement and are the suggested method of use.  They expose methods for saving both the generated workflows and their respective properties.  Files generated via these traits will be named:

* `workflow.xml`
* `coordinator.xml`
* `job.properties`

and deposited in the specified folder.

## Job properties
In addition to generating workflows and coordinators, Scoozie will generate their corresponding job properties.  The property names in these files are also generated and follow the pattern: `actionName_propertyType` for example `someJavaAction_mainClass`.


### Example

The code below shows a worked example of a Scoozie client:

```scala
class TestJob(jobTracker: String, nameNode: String, yarnProperties: Map[String, String])
    extends ScoozieWorkflow
    with ScoozieCoordinator {

  private implicit val credentials: Option[Credentials] = Scoozie.Configuration.emptyCredentials
  
  private val yarnConfig = Scoozie.Configuration.yarnConfiguration(jobTracker, nameNode)
  private val kill = Scoozie.Actions.kill("Workflow failed")

  private val sparkAction = Scoozie.Actions.spark(name = "doASparkThing",
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
                                                  configuration = Scoozie.Configuration.emptyConfiguration,
                                                  yarnConfig = yarnConfig)

  private val emailAction = Scoozie.Actions.email(name = "alertFailure",
                                                  to = Seq("a@a.com", "b@b.com"),
                                                  subject = "message subject",
                                                  body = "message body")

  private val shellAction = Scoozie.Actions.shell(name = "doAShellThing",
                                                  prepareOption = None,
                                                  scriptName = "script.sh",
                                                  scriptLocation = "/path/to/script.sh",
                                                  commandLineArgs = Seq(),
                                                  envVars = Seq(),
                                                  files = Seq(),
                                                  captureOutput = true,
                                                  configuration = Scoozie.Configuration.emptyConfiguration,
                                                  yarnConfig = yarnConfig)

  private val hiveAction = Scoozie.Actions.hive(name = "doAHiveThing",
                                                hiveSettingsXML = "/path/to/settings.xml",
                                                scriptName = "someScript.hql",
                                                scriptLocation = "/path/to/someScript.hql",
                                                parameters = Seq(),
                                                prepareOption = None,
                                                configuration = Scoozie.Configuration.emptyConfiguration,
                                                yarnConfig = yarnConfig)

  private val javaAction = Scoozie.Actions.java(name = "doAJavaThing",
                                                mainClass = "org.antipathy.Main",
                                                javaJar = "/path/to/jar",
                                                javaOptions = "java options",
                                                commandLineArgs = Seq(),
                                                captureOutput = false,
                                                files = Seq(),
                                                prepareOption =
                                                  Scoozie.Prepare.prepare(Seq(Scoozie.Prepare.delete("/some/path"))),
                                                configuration = Scoozie.Configuration.emptyConfiguration,
                                                yarnConfig = yarnConfig)

  private val start = Scoozie.Actions.start

  private val transitions = {
    val errorMail = emailAction okTo kill errorTo kill
    val mainJoin = Scoozie.Actions.join("mainJoin", Scoozie.Actions.end)
    val java = javaAction okTo mainJoin errorTo errorMail
    val hive = hiveAction okTo mainJoin errorTo errorMail
    val mainFork = Scoozie.Actions.fork("mainFork", Seq(java, hive))
    val shell = shellAction okTo mainFork errorTo errorMail
    val spark = sparkAction okTo mainFork errorTo errorMail
    val decision = Scoozie.Actions.decision("sparkOrShell", spark, Scoozie.Actions.switch(shell, "${someVar}"))
    start okTo decision
  }

  override val workflow: Workflow = Scoozie.workflow(name = "ExampleWorkflow",
                                                     path = "/path/to/workflow.xml", //in HDFS
                                                     transitions = transitions,
                                                     configuration = Scoozie.Configuration.emptyConfiguration,
                                                     yarnConfig = yarnConfig)

  override val coordinator: Coordinator = Scoozie.coordinator(name = "exampleCoordinator",
                                                              frequency = "startFreq",
                                                              start = "start",
                                                              end = "end",
                                                              timezone = "timeZome",
                                                              workflow = workflow,
                                                              configuration =
                                                                Scoozie.Configuration.configuration(yarnProperties))
}
```

The artifacts can be generated from this class via the following methods:

```scala
testJob.saveCoordinator("/some/path/")
```
 or
 
```scala
 testJob.saveWorkflow("/some/path/")
```
 
 As mentioned above, this would save both the xml and the required properties to the specified location.  THe properties generated from this example would be:
 
```
alertFailure_body=message body
alertFailure_subject=message subject
alertFailure_to=a@a.com,b@b.com
doAHiveThing_hiveSettingsXML=/path/to/settings.xml
doAHiveThing_scriptLocation=/path/to/someScript.hql
doAHiveThing_scriptName=someScript.hql
doAJavaThing_javaJar=/path/to/jar
doAJavaThing_javaOptions=java options
doAJavaThing_mainClass=org.antipathy.Main
doAJavaThing_prepare_delete=/some/path
doAShellThing_scriptLocation=/path/to/script.sh
doAShellThing_scriptName=script.sh
doASparkThing_mainClass=org.antipathy.Main
doASparkThing_sparkJar=/path/to/jar
doASparkThing_sparkJobName=JobName
doASparkThing_sparkMasterURL=masterURL
doASparkThing_sparkMode=mode
doASparkThing_sparkOptions=spark options
doASparkThing_sparkSettings=/path/to/spark/settings
exampleCoordinator_property0=value1
exampleCoordinator_property1=value2
jobTracker=yarn
nameNode=nameservice1
```

The transitions from this class would be expressed as 

```scala
start -> sparkOrShell -> doASparkThing -> mainFork -> (doAJavaThing, doAHiveThing) -> mainJoin -> end
```

