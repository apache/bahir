#!/usr/bin/env bash

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

function exit_with_usage {
  cat << EOF

release-build - Creates build distributions from a git commit hash or from HEAD.

SYNOPSIS

usage: release-build.sh [--release-prepare | --release-publish | --release-snapshot]

DESCRIPTION

Use maven infrastructure to create a project release package and publish
to staging release location (https://dist.apache.org/repos/dist/dev/bahir)
and maven staging release repository.

--release-prepare --releaseVersion="2.0.0" --developmentVersion="2.1.0-SNAPSHOT" [--releaseRc="rc1"] [--tag="v2.0.0"] [--gitCommitHash="a874b73"]
This form execute maven release:prepare and upload the release candidate distribution
to the staging release location.

--release-publish --gitCommitHash="a874b73"
Publish the maven artifacts of a release to the Apache staging maven repository.
Note that this will publish both Scala 2.10 and 2.11 artifacts.

--release-snapshot [--gitCommitHash="a874b73"]
Publish the maven snapshot artifacts to Apache snapshots maven repository
Note that this will publish both Scala 2.10 and 2.11 artifacts.

OPTIONS

--releaseVersion     - Release identifier used when publishing
--developmentVersion - Release identifier used for next development cyce
--releaseRc          - Release RC identifier used when publishing, default 'rc1'
--tag                - Release Tag identifier used when taging the release, default 'v$releaseVersion'
--gitCommitHash      - Release tag or commit to build from, default master HEAD
--dryRun             - Dry run only, mostly used for testing.

A GPG passphrase is expected as an environment variable

GPG_PASSPHRASE - Passphrase for GPG key used to sign release

EXAMPLES

release-build.sh --release-prepare --releaseVersion="2.0.0" --developmentVersion="2.1.0-SNAPSHOT"
release-build.sh --release-prepare --releaseVersion="2.0.0" --developmentVersion="2.1.0-SNAPSHOT" --releaseRc="rc1" --tag="v2.0.0"
release-build.sh --release-prepare --releaseVersion="2.0.0" --developmentVersion="2.1.0-SNAPSHOT" --releaseRc="rc1" --tag="v2.0.0"  --gitCommitHash="a874b73" --dryRun

release-build.sh --release-publish --gitCommitHash="a874b73"

release-build.sh --release-snapshot
release-build.sh --release-snapshot --gitCommitHash="a874b73"

EOF
  exit 1
}

set -e

if [ $# -eq 0 ]; then
  exit_with_usage
fi


# Process each provided argument configuration
while [ "${1+defined}" ]; do
  IFS="=" read -ra PARTS <<< "$1"
  case "${PARTS[0]}" in
    --release-prepare)
      GOAL="release-prepare"
      RELEASE_PREPARE=true
      shift
      ;;
    --release-publish)
      GOAL="release-publish"
      RELEASE_PUBLISH=true
      shift
      ;;
    --release-snapshot)
      GOAL="release-snapshot"
      RELEASE_SNAPSHOT=true
      shift
      ;;
    --gitCommitHash)
      GIT_REF="${PARTS[1]}"
      shift
      ;;
    --releaseVersion)
      RELEASE_VERSION="${PARTS[1]}"
      shift
      ;;
    --developmentVersion)
      DEVELOPMENT_VERSION="${PARTS[1]}"
      shift
      ;;
    --releaseRc)
      RELEASE_RC="${PARTS[1]}"
      shift
      ;;
    --tag)
      RELEASE_TAG="${PARTS[1]}"
      shift
      ;;
    --dryRun)
      DRY_RUN="-DdryRun=true"
      shift
      ;;

    *help* | -h)
      exit_with_usage
     exit 0
     ;;
    -*)
     echo "Error: Unknown option: $1" >&2
     exit 1
     ;;
    *)  # No more options
     break
     ;;
  esac
done


for env in GPG_PASSPHRASE; do
  if [ -z "${!env}" ]; then
    echo "ERROR: $env must be set to run this script"
    exit_with_usage
  fi
done

if [[ "$RELEASE_PREPARE" == "true" && -z "$RELEASE_VERSION" ]]; then
    echo "ERROR: --releaseVersion must be passed as an argument to run this script"
    exit_with_usage
fi

if [[ "$RELEASE_PREPARE" == "true" && -z "$DEVELOPMENT_VERSION" ]]; then
    echo "ERROR: --developmentVersion must be passed as an argument to run this script"
    exit_with_usage
fi

if [[ "$RELEASE_PUBLISH" == "true" && -z "$GIT_REF" ]]; then
    echo "ERROR: --gitCommitHash must be passed as an argument to run this script"
    exit_with_usage
fi

if [[ "$RELEASE_PUBLISH" == "true" && "$DRY_RUN" ]]; then
    echo "ERROR: --dryRun not supported for --release-publish"
    exit_with_usage
fi

if [[ "$RELEASE_SNAPSHOT" == "true" && "$DRY_RUN" ]]; then
    echo "ERROR: --dryRun not supported for --release-publish"
    exit_with_usage
fi

# Commit ref to checkout when building
GIT_REF=${GIT_REF:-master}

BASE_DIR=$(pwd)

MVN="mvn"
PUBLISH_PROFILES="-Pdistribution"

if [ -z "$RELEASE_RC" ]; then
  RELEASE_RC="rc1"
fi

if [ -z "$RELEASE_TAG" ]; then
  RELEASE_TAG="v$RELEASE_VERSION-$RELEASE_RC"
fi

RELEASE_STAGING_LOCATION="https://dist.apache.org/repos/dist/dev/bahir/"


echo "  "
echo "-------------------------------------------------------------"
echo "------- Release preparation with the following parameters ---"
echo "-------------------------------------------------------------"
echo "Executing           ==> $GOAL"
echo "Git reference       ==> $GIT_REF"
echo "release version     ==> $RELEASE_VERSION"
echo "development version ==> $DEVELOPMENT_VERSION"
echo "rc                  ==> $RELEASE_RC"
echo "tag                 ==> $RELEASE_TAG"
if [ "$DRY_RUN" ]; then
   echo "dry run ?           ==> true"
fi
echo "  "
echo "Deploying to :"
echo $RELEASE_STAGING_LOCATION
echo "  "

function checkout_code {
    # Checkout code
    rm -rf target
    mkdir target
    cd target
    rm -rf bahir
    git clone https://git-wip-us.apache.org/repos/asf/bahir.git
    cd bahir
    git checkout $GIT_REF
    git_hash=`git rev-parse --short HEAD`
    echo "Checked out Bahir git hash $git_hash"

    git clean -d -f -x
    #rm .gitignore
    #rm -rf .git

    cd "$BASE_DIR" #return to base dir
}

if [[ "$RELEASE_PREPARE" == "true" ]]; then
    echo "Preparing release $RELEASE_VERSION"
    # Checkout code
    checkout_code
    cd target/bahir

    # Build and prepare the release
    $MVN $PUBLISH_PROFILES release:clean release:prepare $DRY_RUN -Dgpg.passphrase="$GPG_PASSPHRASE" -DskipTests -DreleaseVersion="$RELEASE_VERSION" -DdevelopmentVersion="$DEVELOPMENT_VERSION" -Dtag="$RELEASE_TAG"

    cd .. #exit bahir

    if [ -z "$DRY_RUN" ]; then
        svn co $RELEASE_STAGING_LOCATION svn-bahir
        mkdir -p svn-bahir/$RELEASE_VERSION-$RELEASE_RC
        cp bahir/distribution/target/*.tar.gz svn-bahir/$RELEASE_VERSION-$RELEASE_RC/
        cp bahir/distribution/target/*.zip    svn-bahir/$RELEASE_VERSION-$RELEASE_RC/

        cd svn-bahir/$RELEASE_VERSION-$RELEASE_RC/
        for i in *.zip *.gz; do gpg --output $i.asc --detach-sig --armor $i; done
        for i in *.zip *.gz; do openssl md5 -hex $i | sed 's/MD5(\([^)]*\))= \([0-9a-f]*\)/\2 *\1/' > $i.md5; done

        cd .. #exit $RELEASE_VERSION-$RELEASE_RC/

        svn add $RELEASE_VERSION-$RELEASE_RC/
        svn ci -m"Apache Bahir $RELEASE_VERSION-$RELEASE_RC"
    fi


    cd "$BASE_DIR" #exit target

    exit 0
fi


if [[ "$RELEASE_PUBLISH" == "true" ]]; then
    echo "Preparing release $RELEASE_VERSION"
    # Checkout code
    checkout_code
    cd target/bahir

    #Deploy default scala 2.11
    mvn -DaltDeploymentRepository=apache.releases.https::default::https://repository.apache.org/service/local/staging/deploy/maven2 clean verify gpg:sign install:install deploy:deploy -DskiptTests -Dgpg.passphrase=$GPG_PASSPHRASE $PUBLISH_PROFILES

    #Deploy scala 2.10
    ./dev/change-scala-version.sh 2.10
    mvn -DaltDeploymentRepository=apache.releases.https::default::https://repository.apache.org/service/local/staging/deploy/maven2 clean verify gpg:sign install:install deploy:deploy -DskiptTests -Dscala-2.10 -Dgpg.passphrase=$GPG_PASSPHRASE $PUBLISH_PROFILES

    cd "$BASE_DIR" #exit target

    exit 0
fi


if [[ "$RELEASE_SNAPSHOT" == "true" ]]; then
    # Checkout code
    checkout_code
    cd target/bahir

    CURRENT_VERSION=$($MVN help:evaluate -Dexpression=project.version \
    | grep -v INFO | grep -v WARNING | grep -v Download)

    # Publish Bahir Snapshots to Maven snapshot repo
    echo "Deploying Bahir SNAPSHOT at '$GIT_REF' ($git_hash)"
    echo "Publish version is $CURRENT_VERSION"
    if [[ ! $CURRENT_VERSION == *"SNAPSHOT"* ]]; then
        echo "ERROR: Snapshots must have a version containing SNAPSHOT"
        echo "ERROR: You gave version '$CURRENT_VERSION'"
        exit 1
    fi

    #Deploy default scala 2.11
    $MVN -DaltDeploymentRepository=apache.snapshots.https::default::https://repository.apache.org/content/repositories/snapshots clean verify gpg:sign install:install deploy:deploy -DskiptTests -Dgpg.passphrase=$GPG_PASSPHRASE $PUBLISH_PROFILES

    #Deploy scala 2.10
    ./dev/change-scala-version.sh 2.10
    $MVN -DaltDeploymentRepository=apache.snapshots.https::default::https://repository.apache.org/content/repositories/snapshots clean verify gpg:sign install:install deploy:deploy -DskiptTests -Dscala-2.10 -Dgpg.passphrase=$GPG_PASSPHRASE $PUBLISH_PROFILES

    cd "$BASE_DIR" #exit target
    exit 0
fi


cd "$BASE_DIR" #return to base dir
rm -rf target
echo "ERROR: wrong execution goals"
exit_with_usage
