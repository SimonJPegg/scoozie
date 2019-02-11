# Scoozie

[![Codacy][codacyImg2]][codacyLink]  [![CodeCovImg]][CodCovLink]  [![languageBage]][languageLink] 

[![issuesBadge]][issuesLink] [![releaseBadge]][releaseLink] [![newCommitsBadge]][newCommitsLink]

Latest versions: 




* Scala 2.10:  [![Maven][210mavenImg]][mavenLink]
* Scala 2.11:  [![Maven][211mavenImg]][mavenLink]
* Scala 2.12:  [![Maven][212mavenImg]][mavenLink]

An Oozie artefact builder library for scala.  it was created to allow developers  to quickly generate new Oozie artefacts without copy/pasting xml and avoiding typos in the XML they create.

Scoozie was created with CDH in mind and supports oozie version 4.1.0.


## Usage 
Scoozie provides two modes of interaction

* A [Scala API](./ScalaAPI.md)
* A [Hocon API](HoconAPI.md)

See the above links for more information.

## TODO

* Add support for Oozie SLAs
* Add the ability to join `ScoozieWorkflow` classes and combine properties.
* Add functionality to deploy artefacts directly from the library.
* Add support for complex workflows in the `WorkflowTestRunner` class.
* Add support for datasets.

[languageBage]: https://img.shields.io/github/languages/top/simonjpegg/scoozie.svg?style=flat
[languageLink]: https://www.scala-lang.org/

[issuesBadge]: https://img.shields.io/github/issues/simonjpegg/scoozie.svg?style=flat
[issuesLink]: https://github.com/SimonJPegg/scoozie/issues

[licenseImg]: https://img.shields.io/badge/Licence-Apache%202.0-blue.svg
[licenseLink]: ./LICENSE

[codacyImg]: https://img.shields.io/codacy/grade/fdf40afa99a342b093836bfa22871c2d.svg?style=flat
[codacyImg2]: https://api.codacy.com/project/badge/grade/fdf40afa99a342b093836bfa22871c2d
[codacyLink]: https://app.codacy.com/project/SimonJPegg/scoozie/dashboard

[210mavenImg]: https://maven-badges.herokuapp.com/maven-central/org.antipathy/scoozie_2.10/badge.svg
[211mavenImg]: https://maven-badges.herokuapp.com/maven-central/org.antipathy/scoozie_2.11/badge.svg
[212mavenImg]: https://maven-badges.herokuapp.com/maven-central/org.antipathy/scoozie_2.12/badge.svg
[mavenLink]: https://search.maven.org/search?q=scoozie

[CodeCovImg]: https://api.codacy.com/project/badge/Coverage/4c627c7c58834629a0d737db4097a1b0
[CodCovLink]: https://www.codacy.com?utm_source=github.com&utm_medium=referral&utm_content=SimonJPegg/scoozie&utm_campaign=Badge_Coverage

[releaseBadge]: https://img.shields.io/github/release/simonjpegg/scoozie.svg?style=flat
[releaseLink]: https://github.com/SimonJPegg/scoozie/releases

[newCommitsBadge]: https://img.shields.io/github/commits-since/simonjpegg/scoozie/latest.svg?style=flat
[newCommitsLink]: https://github.com/SimonJPegg/scoozie/commits/master