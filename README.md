# Apache Bahir

Apache Bahir provides extensions to distributed analytics platforms such as Apache Spark.

<http://bahir.apache.org/>

## Apache Bahir origins

The Initial Bahir source code (see issue [BAHIR-1](https://issues.apache.org/jira/browse/BAHIR-1)) containing the source for the Apache Spark streaming connectors for akka, mqtt, twitter, zeromq
extracted from [Apache Spark revision 8301fad](https://github.com/apache/spark/tree/8301fadd8d269da11e72870b7a889596e3337839)
(before the [deletion of the streaming connectors akka, mqtt, twitter, zeromq](https://issues.apache.org/jira/browse/SPARK-13843)). 

## Source Code Structure

Source code folder structure:
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

## Building Bahir

Bahir is built using [Apache Maven](http://maven.apache.org/).
To build Bahir and its example programs, run:

    mvn -DskipTests clean install

## Running Tests

Testing first requires [building Bahir](#building-bahir). Once Bahir is built, tests
can be run using:

    mvn test

## Example Programs

Each extension currently available in Apache Bahir has an example application located under the "examples" folder.


## Online Documentation

Coming Soon.


## A Note About Apache Spark Integration

Coming soon.
