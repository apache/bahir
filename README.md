# Bahir Repository

Initial Bahir repository containing the source for the Spark streaming connectors for akka, mqtt, twitter, zeromq 
extracted from [Apache Spark revision 8301fad](https://github.com/apache/spark/tree/8301fadd8d269da11e72870b7a889596e3337839)
(before the [deletion of the streaming connectors akka, mqtt, twitter, zeromq](https://issues.apache.org/jira/browse/SPARK-13843)). 

This repo still needs license files, build scripts, READMEs, etc but it does have all of 
the commit history as well as the respective examples (with rewritten revision trees to preserve pre-Bahir history).


Folder structure:
```
- streaming-akka
  - examples/src/main/...
  - src/main/...
- streaming-mqtt
  - examples
  - src
  - python
- ...
```
