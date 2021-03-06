# project version
<p align="left">
  <a href="https://mvnrepository.com/artifact/com.github.maketubo/liquibase-hive/1.0.0">
    <img alt="maven" src="https://img.shields.io/maven-central/v/com.github.maketubo/liquibase-hive/1.0.0">
  </a>

  <a href="https://www.apache.org/licenses/LICENSE-2.0">
    <img alt="license" src="https://img.shields.io/badge/license-Apache%202-4EB1BA.svg?style=flat-square">
  </a>
  <a href="pleaseSendAMail2Me">
    <img alt="email" src="https://badges.gitter.im/Join%20Chat.svg">
  </a>
</p>

# dependency version
[![liquibase](https://img.shields.io/badge/liquibase-3.8.4-blue)](https://shields.io/) [![hive-jdbc](https://img.shields.io/badge/hive--jdbc-2.0.1-yellowgreen)](https://shields.io/) 

# About liquibase-hive
Liquibase-hive is a [Liquibase](http://www.liquibase.org/) [extension](https://liquibase.jira.com/wiki/spaces/CONTRIB/overview), which adds support for Apache Hive.
Because the old project has not been updated for a long time, so we fork it and will continue to update.

# How to use
```
<dependency>
    <groupId>com.github.maketubo</groupId>
    <artifactId>liquibase-hive</artifactId>
    <version>1.0.0</version>
</dependency>
```


## Liquibase-hive specific configuration

Liquibase-hive provides additional configuration parameters that can be used to influence its behaviour:

| parameter         | values                | description                                       |
| ----------------- | --------------------- | ------------------------------------------------- |
| liquibase.lock    | true (default), false | enables/disables locking facility for a given job |

