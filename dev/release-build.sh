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

--release-prepare --releaseVersion="2.4.0" --developmentVersion="3.0.0-SNAPSHOT" [--tag="v2.4.0-rc1"] [--gitBranch="branch-2.4"]
This form execute maven release:prepare and upload the release candidate distribution
to the staging release location.

--release-publish --tag="v2.4.0-rc1"
Publish the maven artifacts of a release to the Apache staging maven repository.
Note that this will publish both Scala 2.11 and 2.12 artifacts.

--release-snapshot
Publish the maven snapshot artifacts to Apache snapshots maven repository
Note that this will publish both Scala 2.11 and 2.12 artifacts.

OPTIONS

--releaseVersion     - Release identifier used when publishing
--developmentVersion - Release identifier used for next development cyce
--tag                - Release Tag identifier used when taging the release, default 'v$releaseVersion'
--gitBranch          - Release branch used when checking out the code to be released
--gitCommitHash      - Release tag or commit to build from, default master HEAD
--dryRun             - Dry run only, mostly used for testing.

A GPG passphrase is expected as an environment variable

GPG_PASSPHRASE - Passphrase for GPG key used to sign release

EXAMPLES

release-build.sh --release-prepare --releaseVersion="2.4.0" --developmentVersion="3.0.0-SNAPSHOT" --tag="v2.4.0-rc1"
release-build.sh --release-prepare --releaseVersion="2.4.0" --developmentVersion="3.0.0-SNAPSHOT" --tag="v2.4.0-rc1" --gitBranch="branch-2.4"
release-build.sh --release-prepare --releaseVersion="2.4.0" --developmentVersion="3.0.0-SNAPSHOT" --tag="v2.4.0-rc1" --gitBranch="branch-2.4"  --dryRun

release-build.sh --release-publish --gitTag="v2.4.0-rc1"

release-build.sh --release-snapshot

EOF
  exit 1
}

set -e

if [ $# -eq 0 ]; then
  exit_with_usage
fi


# process each provided argument configuration
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
      GIT_HASH="${PARTS[1]}"
      shift
      ;;
    --gitBranch)
      GIT_BRANCH="${PARTS[1]}"
      shift
      ;;
    --gitTag)
      GIT_TAG="${PARTS[1]}"
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
    *)  # no more options
     break
     ;;
  esac
done


if [[ -z "$GPG_PASSPHRASE" ]]; then
    echo 'The environment variable GPG_PASSPHRASE is not set. Enter the passphrase to'
    echo 'unlock the GPG signing key that will be used to sign the release!'
    echo
    stty -echo && printf "GPG passphrase: " && read GPG_PASSPHRASE && printf '\n' && stty echo
  fi

if [[ "$RELEASE_PREPARE" == "true" && -z "$RELEASE_VERSION" ]]; then
    echo "ERROR: --releaseVersion must be passed as an argument to run this script"
    exit_with_usage
fi

if [[ "$RELEASE_PREPARE" == "true" && -z "$DEVELOPMENT_VERSION" ]]; then
    echo "ERROR: --developmentVersion must be passed as an argument to run this script"
    exit_with_usage
fi

if [[ "$RELEASE_PREPARE" == "true"  ]]; then
    if [[ "$GIT_HASH" && "$GIT_BRANCH" ]]; then
        echo "ERROR: Only one argument permitted when publishing : --gitCommitHash or --gitBranch"
        exit_with_usage
    fi
fi

if [[ "$RELEASE_PUBLISH" == "true"  ]]; then
    if [[ "$GIT_HASH" && "$GIT_TAG" ]]; then
        echo "ERROR: Only one argument permitted when publishing : --gitCommitHash or --gitTag"
        exit_with_usage
    fi
    if [[ -z "$GIT_HASH" && -z "$GIT_TAG" ]]; then
        echo "ERROR: --gitCommitHash OR --gitTag must be passed as an argument to run this script"
        exit_with_usage
    fi
fi

if [[ "$RELEASE_PUBLISH" == "true" && "$GIT_HASH" ]]; then
    echo "ERROR: --gitCommitHash not supported for --release-publish"
    exit_with_usage
fi

if [[ "$RELEASE_PUBLISH" == "true" && "$DRY_RUN" ]]; then
    echo "ERROR: --dryRun not supported for --release-publish"
    exit_with_usage
fi

if [[ "$RELEASE_SNAPSHOT" == "true" && "$DRY_RUN" ]]; then
    echo "ERROR: --dryRun not supported for --release-snapshot"
    exit_with_usage
fi

if [[ "$RELEASE_PREPARE" == "true" || "$RELEASE_PUBLISH" == "true" ]]; then
  if [ -z "$RELEASE_TAG" ]; then
    echo "ERROR: --tag must be passed as an argument to run this script"
    exit_with_usage
  fi
fi

# commit ref to checkout when building
GIT_HASH=${GIT_HASH:-master}
if [[ "$RELEASE_PREPARE" == "true" && "$GIT_BRANCH" ]]; then
    GIT_HASH="origin/$GIT_BRANCH"
fi
if [[ "$RELEASE_PUBLISH" == "true" && "$GIT_TAG" ]]; then
    GIT_HASH="tags/$GIT_TAG"
fi

BASE_DIR=$(pwd)

MVN="mvn"
PUBLISH_PROFILES="-Pdistribution"


RELEASE_STAGING_LOCATION="https://dist.apache.org/repos/dist/dev/bahir/bahir-spark"

echo "  "
echo "-----------------------------------------------------------------"
echo "------- Release preparation with the following parameters -------"
echo "-----------------------------------------------------------------"
echo "Executing           ==> $GOAL"
echo "Git reference       ==> $GIT_HASH"
echo "Release version     ==> $RELEASE_VERSION"
echo "Development version ==> $DEVELOPMENT_VERSION"
echo "Tag                 ==> $RELEASE_TAG"
if [ "$DRY_RUN" ]; then
   echo "dry run ?           ==> true"
fi
echo "  "
echo "Deploying to :"
echo $RELEASE_STAGING_LOCATION
echo "  "

function checkout_code {
    rm -rf target
    mkdir target
    cd target
    rm -rf bahir
    git clone https://git-wip-us.apache.org/repos/asf/bahir.git
    cd bahir
    git checkout $GIT_HASH
    git_hash=`git rev-parse --short HEAD`
    echo "Checked out Bahir git hash $git_hash"

    cd "$BASE_DIR" # return to base dir
}

if [[ "$RELEASE_PREPARE" == "true" ]]; then
    echo "Preparing release $RELEASE_VERSION"
    # checkout code
    checkout_code
    cd target/bahir

    # test with scala 2.11 and 2.12
    ./dev/change-scala-version.sh 2.11
    $MVN $PUBLISH_PROFILES clean test -Dscala-2.11 || exit 1
    ./dev/change-scala-version.sh 2.12
    $MVN $PUBLISH_PROFILES clean test || exit 1

    # build and prepare the release
    $MVN $PUBLISH_PROFILES release:clean release:prepare $DRY_RUN \
        -DskipTests=true -Dgpg.passphrase="$GPG_PASSPHRASE" \
        -DreleaseVersion="$RELEASE_VERSION" -DdevelopmentVersion="$DEVELOPMENT_VERSION" -Dtag="$RELEASE_TAG"

    cd .. # exit bahir

    if [ -z "$DRY_RUN" ]; then
        cd "$BASE_DIR/target/bahir"
        git checkout $RELEASE_TAG
        git clean -d -f -x

        $MVN $PUBLISH_PROFILES clean install -DskipTests=true

        cd "$BASE_DIR/target"
        svn co $RELEASE_STAGING_LOCATION svn-bahir
        mkdir -p svn-bahir/$RELEASE_VERSION-$RELEASE_RC

        cp bahir/distribution/target/*.tar.gz svn-bahir/$RELEASE_VERSION-$RELEASE_RC/
        cp bahir/distribution/target/*.zip svn-bahir/$RELEASE_VERSION-$RELEASE_RC/

        cd svn-bahir/$RELEASE_VERSION-$RELEASE_RC/
        rm -f *.asc
        for i in *.zip *.tar.gz; do gpg --output $i.asc --detach-sig --armor $i; done
        rm -f *.sha*
        for i in *.zip *.tar.gz; do shasum --algorithm 512 $i > $i.sha512; done

        cd .. # exit $RELEASE_VERSION-$RELEASE_RC

        svn add $RELEASE_VERSION-$RELEASE_RC/
        svn ci -m"Apache Bahir $RELEASE_VERSION-$RELEASE_RC"
    fi

    cd "$BASE_DIR" # exit target
    exit 0
fi


if [[ "$RELEASE_PUBLISH" == "true" ]]; then
    echo "Preparing release $RELEASE_VERSION"
    # checkout code
    checkout_code
    cd target/bahir

    DEPLOYMENT_REPOSITORY="apache.releases.https::default::https://repository.apache.org/service/local/staging/deploy/maven2"

    # deploy default scala 2.12
    $MVN $PUBLISH_PROFILES clean package gpg:sign install:install deploy:deploy \
        -DaltDeploymentRepository=$DEPLOYMENT_REPOSITORY \
        -DskipTests=true -Dgpg.passphrase=$GPG_PASSPHRASE

    # deploy scala 2.11
    ./dev/change-scala-version.sh 2.11
    $MVN $PUBLISH_PROFILES clean package gpg:sign install:install deploy:deploy \
        -DaltDeploymentRepository=$DEPLOYMENT_REPOSITORY \
        -DskipTests=true -Dgpg.passphrase=$GPG_PASSPHRASE -Dscala-2.11

    cd "$BASE_DIR" # exit target
    exit 0
fi


if [[ "$RELEASE_SNAPSHOT" == "true" ]]; then
    # checkout code
    checkout_code
    cd target/bahir

    DEPLOYMENT_REPOSITORY="apache.snapshots.https::default::https://repository.apache.org/content/repositories/snapshots"
    CURRENT_VERSION=$($MVN help:evaluate -Dexpression=project.version | grep -v INFO | grep -v WARNING | grep -v Download)

    # publish Bahir snapshots to maven repository
    echo "Deploying Bahir SNAPSHOT at '$GIT_HASH' ($git_hash)"
    echo "Publish version is $CURRENT_VERSION"
    if [[ ! $CURRENT_VERSION == *"SNAPSHOT"* ]]; then
        echo "ERROR: Snapshots must have a version containing SNAPSHOT"
        echo "ERROR: You gave version '$CURRENT_VERSION'"
        exit 1
    fi

    # deploy default scala 2.12
    $MVN $PUBLISH_PROFILES clean package gpg:sign install:install deploy:deploy \
        -DaltDeploymentRepository=$DEPLOYMENT_REPOSITORY \
        -DskipTests=true -Dgpg.passphrase=$GPG_PASSPHRASE

    # deploy scala 2.11
    ./dev/change-scala-version.sh 2.11
    $MVN $PUBLISH_PROFILES clean package gpg:sign install:install deploy:deploy \
        -DaltDeploymentRepository=$DEPLOYMENT_REPOSITORY \
        -DskipTests=true -Dgpg.passphrase=$GPG_PASSPHRASE -Dscala-2.11

    cd "$BASE_DIR" # exit target
    exit 0
fi


cd "$BASE_DIR" # return to base directory
rm -rf target
echo "ERROR: wrong execution goals"
exit_with_usage
