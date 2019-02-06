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

GIT_APACHE_URL=https://git-wip-us.apache.org/repos/asf/bahir.git
GIT_GITHUB_URL=https://github.com/apache/bahir.git
TARGET_BRANCH=master

JIRA_URL=https://issues.apache.org/jira

base_dir=$(pwd)
base_ifs=${IFS}
work_dir=${base_dir}/pr-$1

function print_usage {
  cat << EOF

merge-pr - Merges given pull request and resolves associated JIRA ticket.

SYNOPSIS

usage: merge-pr.sh [pull-request-ID]

DESCRIPTION

Merges given pull request and resolves associated JIRA ticket. Message of the
most recent commit should contain reference to JIRA ticket in square brackets,
e.g. [BAHIR-123] Support SSL in MQTT.

OPTIONS

pull-request-ID     - Number of the pull request

EXAMPLES

merge-pr.sh 77

EOF
}

function init() {
    rm -rf ${work_dir}
    mkdir ${work_dir}
    cd ${work_dir}
}

function exit_with_code() {
    cd ${base_dir}
    rm -rf ${work_dir}
    exit $1
}

function merge_git_pr() {
    local pull_id=$1
    local jira_issue=$2

    # Pull fresh repository and fetch branch containing pull request.
    git clone ${GIT_APACHE_URL} --quiet .
    git remote add apache ${GIT_GITHUB_URL}
    git fetch apache pull/${pull_id}/head:pr-${pull_id} --quiet 2>/dev/null
    if [[ $? != 0 ]]; then
        echo "Pull request #${pull_id} does not exist."
        exit_with_code 1
    fi
    git checkout pr-${pull_id} --quiet

    # Print summary.
    IFS=$'\n';
    local commit_list=( $(git log origin/${TARGET_BRANCH}..pr-${pull_id} --oneline) )
    IFS=${base_ifs}
    if [[ ${commit_list[@]} == "" ]]; then
        echo "No commits to merge."
        exit_with_code 0
    fi
    echo "Commits to be merged:"
    for i in ${!commit_list[@]}
    do
        echo "   ${commit_list[$i]}"
    done
    if [[ ${#commit_list[@]} > 1 ]]; then
        echo "Maybe ask contributor to squash commits."
        read -r -p "Continue \"as is\"? [y/n] " confirmation
        if [[ ${confirmation} != "y" ]]; then
            exit_with_code 0
        fi
    else
        read -r -p "Continue? [y/n] " confirmation
        if [[ ${confirmation} != "y" ]]; then
            exit_with_code 0
        fi
    fi

    local last_commit_msg=$(git log -1 --pretty=%B)
    read -r -p "Close pull request #${pull_id}? [y/n] " confirmation
    if [[ ${confirmation} == "y" ]]; then
        local suffix=$(printf "\n\nCloses #${pull_id}")
        git commit --quiet --amend -m "${last_commit_msg}${suffix}"
    fi

    git rebase ${TARGET_BRANCH} --quiet
    if [[ $? != 0 ]]; then
        # Fail in case of conflicts.
        git merge --abort
        echo "Branch contains conflicts. Ask contributor to rebase with master."
        exit_with_code 1
    fi

    git checkout ${TARGET_BRANCH} --quiet
    git merge --quiet pr-${pull_id}
    git push --quiet origin ${TARGET_BRANCH}
    echo "Commits pushed to origin/${TARGET_BRANCH}."

    eval ${jira_issue}=$(echo ${last_commit_msg} | awk -F'[' '{print $2}' | awk -F']' '{print $1}')
}

function update_and_resolve_jira {
    local jira_project=$1
    local jira_ticket_number=$2

    read -r -p "Approver JIRA user: " jira_user
    read -s -p "Approver JIRA password: " jira_pass
    echo

    # Lookup JIRA ticket details.
    local ticket_details=$(curl --silent --write-out "%{http_code}" --user ${jira_user}:${jira_pass} \
        --request GET --url ${JIRA_URL}/rest/api/2/issue/${jira_project}-${jira_ticket_number})
    local status_code=${ticket_details:(-3)}
    if [[ ${status_code} != 200 ]]; then
        echo "Failed to retrieve JIRA ticket ${jira_project}-${jira_ticket_number} details: ${status_code}."
        exit_with_code 1
    fi
    ticket_details=${ticket_details:0:${#ticket_details}-3}

    # Choose new assignee.
    local current_assignee=$(echo ${ticket_details} | jq --raw-output '.fields.assignee.name')
    if [[ ${current_assignee} == "null" ]]; then
        current_assignee="unassigned"
    fi
    local assign_options=( $(echo ${ticket_details} | \
        jq --raw-output '.fields.assignee.name,.fields.creator.name,.fields.reporter.name,.fields.comment.comments[].author.name' | \
        uniq | grep -v null) )
    assign_options+=("other")

    echo "Specify assignee of JIRA ticket ${jira_project}-${jira_ticket_number} (current: ${current_assignee}):"
    for i in ${!assign_options[*]}
    do
        printf "%4d: %s\n" ${i} ${assign_options[$i]}
    done
    read -r -p "Chosen option: " assign_index
    local new_assignee=${assign_options[${assign_index}]}
    if [[ ${new_assignee} == "other" ]]; then
        read -r -p "Enter JIRA account name: " new_assignee
    fi

    # Specify which release the issues is going to be fixed in.
    local version_options=( $(curl --silent --user ${jira_user}:${jira_pass} \
        --request GET --url ${JIRA_URL}/rest/api/2/project/${jira_project}/versions | \
        jq --raw-output '.[] | select(.released == false) | .name') )

    echo "Specify fixed release version:"
    for i in ${!version_options[*]}
    do
        printf "%4d: %s\n" ${i} ${version_options[$i]}
    done
    read -r -p "Chosen option: " version_index

    # Update JIRA ticket. Set appropriate fixed release version and assignee.
    status_code=$(curl --silent --write-out "%{http_code}" --user ${jira_user}:${jira_pass} \
        --request PUT --url ${JIRA_URL}/rest/api/2/issue/${jira_project}-${jira_ticket_number} \
        --header 'Content-Type: application/json' --data \
        "{
            \"update\": {
                    \"fixVersions\": [ { \"set\": [ { \"name\": \"${version_options[${version_index}]}\" } ] } ]
            },
            \"fields\": { \"assignee\": { \"name\": \"${new_assignee}\" } }
        }")
    if [[ ${status_code} != 204 ]]; then
        echo "Failed to update JIRA ticket with given assignee and release version: ${status_code}."
        exit_with_code 1
    fi

    # Update JIRA ticket. Transition the ticket to resolved state
    # (id = 5, checked with GET ..issue/ABC-1/transitions API).
    status_code=$(curl --silent --write-out "%{http_code}" --user ${jira_user}:${jira_pass} \
        --request POST --url ${JIRA_URL}/rest/api/2/issue/${jira_project}-${jira_ticket_number}/transitions \
        --header 'Content-Type: application/json' --data "{ \"transition\": { \"id\": \"5\" } }")
    if [[ ${status_code} != 204 ]]; then
        echo "Failed to transition JIRA ticket to resolved state: ${status_code}."
        exit_with_code 1
    fi

    echo "JIRA ticket successfully updated."
}

if ! [[ $# == 1 && $1 =~ ^[0-9]+$ ]]; then
  print_usage
  exit 1
fi

init
merge_git_pr $1 jira_ticket
update_and_resolve_jira $(echo ${jira_ticket} | cut -d- -f1) $(echo ${jira_ticket} | cut -d- -f2)
exit_with_code 0
