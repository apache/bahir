#!/bin/bash -e
#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
set -o pipefail

# make sure Spark home is set and valid
if [ -z "${SPARK_HOME}" ]; then
  echo "SPARK_HOME is not set" >&2
  exit 1
elif [ ! -d "${SPARK_HOME}" ]; then
  echo "SPARK_HOME does not point to a valid directory" >&2
  exit 1
fi

# pinpoint the module folder and project root folder
bin_dir=$( dirname "$0" )
module_dir=$( cd "${bin_dir}/.." ; pwd -P )
project_dir=$( cd "${module_dir}/.." ; pwd -P )

# use the module name to find the tests jar file that contains the example to run
module_name=${module_dir#"${project_dir}"/}
module_tests_jar_path=$( find "${module_dir}" -name "*${module_name}*-tests.jar" | head -1 )

if [ -z "${module_tests_jar_path}" ] || [ ! -e "${module_tests_jar_path}" ]; then
  echo "Could not find module tests jar file in ${module_dir}/target/" >&2
  echo "Run \"mvn clean install\" and retry running this example" >&2
  exit 1
fi

# use maven-help-plugin to determine project version and Scala version
module_version=$( cd "${module_dir}" && mvn org.apache.maven.plugins:maven-help-plugin:2.2:evaluate -Dexpression=project.version | grep -v "INFO\|WARNING\|ERROR\|Downloading" | tail -1 )
scala_version=$( cd "${module_dir}" && mvn org.apache.maven.plugins:maven-help-plugin:2.2:evaluate -Dexpression=scala.binary.version | grep -v "INFO\|WARNING\|ERROR\|Downloading" | tail -1 )

# we are using spark-submit with --packages to run the tests and all necessary dependencies are
# resolved by maven which requires running "mvn" or "mvn install" first
spark_packages="org.apache.bahir:spark-${module_name}_${scala_version}:${module_version}"

# find additional test-scoped dependencies and add them to the --packages list
test_dependencies=$( mvn dependency:tree -Dscope=test -Dtokens=standard -pl ${module_name} | grep "\[INFO\] +- [a-z].*:test" | grep -ivE "spark|bahir|scala|junit" | sed 's/\[INFO\] +- //; s/:jar//; s/:test//' )
for td in ${test_dependencies}; do
  spark_packages="${spark_packages},${td}"
done

# since we are running locally, we can use PYTHONPATH instead of --py-files
export PYTHONPATH="${module_dir}/python:${PYTHONPATH}"

${SPARK_HOME}/bin/spark-submit \
    --master local[*] \
    --driver-memory 512m \
    --packages "${spark_packages}" \
    --jars "${module_tests_jar_path}" \
    "${module_dir}/python-tests/tests.py"
