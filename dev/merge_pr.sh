#!/usr/bin/env bash
################################################################################
#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
# limitations under the License.
################################################################################

# Script for creating well-formed pull request,  merging and pushing them to 
# https://git.code.oa.com/spark/spark
# This utility assumes you already install jq: https://stedolan.github.io/jq/
#   usage: ./merge_pr.sh {pr_num}
# Before running script, please fill in the following configurations:
# PRIVATE_TOKEN: get it from https://git.code.oa.com/profile/account
# PUSH_REMOTE_NAME: Remote name which points to https://git.code.oa.com/spark/spark


function cleanup() {
    git checkout ${old_branch} --quiet
    if [[ ${has_stashed_changes} == "true" ]]; then
        git stash pop --quiet
    fi

    if [[ -n ${local_target_branch} ]]; then
        git branch -D ${local_target_branch} --quiet
    fi

    if [[ -n ${pr_branch_name} ]]; then
        git branch -D ${pr_branch_name} --quiet
    fi
	
    if [[ -n ${source_repo_name} ]]; then
        git remote remove ${source_repo_name}
    fi
}

function abort() {
    echo "ERROR: $1"
    cleanup
    exit $2
}

function standardize_commit_msg() {
    title=$1
    # Get "STORY-${id}" "BUG-${id}" "Task-${id}"
    tapd_arr[0]=`expr "$title" : '.*\([S|s][T|t][O|o][R|r][Y|y]-[0-9]\+\).*'`
    tapd_arr[1]=`expr "$title" : '.*\([B|b][U|u][G|g]-[0-9]\+\).*'`
    tapd_arr[2]=`expr "$title" : '.*\([T|t][A|a][S|s][K|k]-[0-9]\+\).*'`

    for item in "${tapd_arr[@]}"; do
        if [[ -n $item ]]; then
            # Convert tpad msg in title from lower case to upper case
            upper_item=`echo ${item} | tr 'a-z' 'A-Z'`
            title=${title//$item/$upper_item}

            # Replace '-' with '=' in tapd_msg
            tapd_msg="$tapd_msg --${item//-/=}"

            # Convert upper case to lower case
            tapd_msg=`echo ${tapd_msg} | tr 'A-Z' 'a-z'`
        fi
    done
			
    # Get committer
    committer_msg=`expr "$title" : '.*\(\--user=[a-z]\+\).*'`
    if [[ -n ${committer_msg} ]]; then
        # Remove committer from title to avoid duplicating
        title=${title//${committer_msg}/}
    fi

    # Replace multiple spaces with a single space
    title=${title//'\s+'/' '}

    title=${title}" "${tapd_msg}
    echo ${title}
}

# get PRIVATE_TOKEN https://git.code.oa.com/profile/account 
# PRIVATE_TOKEN=""

# Remote name which points to https://git.code.oa.com/spark/spark
# PUSH_REMOTE_NAME="origin"

# 165505: the repo id of https://git.code.oa.com/spark/spark
TARGET_REPO_REST_URL="https://git.code.oa.com/api/v3/projects/165505"

if [[ $# -eq 0 ]]; then
    echo "ERROR: The merge request number is not specified."
    exit 1
fi

pr_num=$1

if ! [[ ${pr_num} =~ ^[0-9]+$ ]]; then
    echo "ERROR: The format of merge request number is invalid."
    exit 1
fi

if [[ -z ${PRIVATE_TOKEN} ]]; then
    echo "ERROR: The PRIVATE_TOKEN is not specified. You can find your token on" \
        "https://git.code.oa.com/profile/account."
    exit 1
fi

if [[ -z ${PUSH_REMOTE_NAME} ]]; then
    echo "ERROR: The PUSH_REMOTE_NAME is not specified. "
    exit 1
fi

CURL_RESPONSE=$(mktemp -t ".curl-response.XXXX")

printf "Retrieving merge request ${pr_num}..."

status_code=$(jq -n \
    --arg iid "${pr_num}" \
    '{ iid: $iid }' \
    | curl -X GET \
    --header "PRIVATE-TOKEN: ${PRIVATE_TOKEN}" \
    --header "Content-Type: application/json" \
    -d @- \
    --silent \
    --write-out %{http_code} \
    --output ${CURL_RESPONSE} \
    "${TARGET_REPO_REST_URL}/merge_requests")

if [[ ${status_code} -ne 200 ]]; then
    printf "FAILED\n"

    response=$(cat ${CURL_RESPONSE} | jq -r ".message")
    abort "Could not retrieve the merge request. ${response}." ${status_code}
else
    printf "DONE\n"
fi

count=$(cat ${CURL_RESPONSE} | jq -r ". | length")
if [[ ${count} -ne 1 ]]; then
    echo "ERROR: Cannot find the specified merge request."
    exit 1
fi

state=$(cat ${CURL_RESPONSE} | jq -r ".[0].state")
if [[ ${state} != "opened" ]]; then
    echo "ERROR: The merge request is not opened (state: ${state})."
    exit 1
fi

merge_status=$(cat ${CURL_RESPONSE} | jq -r ".[0].merge_status")
if [[ ${merge_status} != "can_be_merged" ]]; then
    echo "ERROR: The merge request can not be merged (status: ${merge_status})."
    exit 1
fi

source_project_id=$(cat ${CURL_RESPONSE} | jq -r ".[0].source_project_id")
source_branch=$(cat ${CURL_RESPONSE} | jq -r ".[0].source_branch")
target_branch=$(cat ${CURL_RESPONSE} | jq -r ".[0].target_branch")
title=$(cat ${CURL_RESPONSE} | jq -r ".[0].title")
author=$(cat ${CURL_RESPONSE} | jq -r ".[0].author.name")
mr_id=$(cat ${CURL_RESPONSE} | jq -r ".[0].id")

# Decide whether to use the modified title or not
modified_title=`standardize_commit_msg "$title"`
if [[ "$modified_title" != "$title" ]]; then
    echo "I've re-written the title as follows to match the standard format:"
    echo "Original: $title"
    echo "Modified: $modified_title"
    read -p "Would you like to use the modified title? (y/n): " accept_modified_title
    if [[ ${accept_modified_title} == 'y' || ${accept_modified_title} == 'Y' ]]; then
        title=$modified_title
    else 
        read -p "Would you like to use the Original title? (y/n): " accept_origin_title
        if [[ ${accept_origin_title} != 'y' && ${accept_origin_title} != 'Y' ]]; then
            read -p "Please input new title: " title
        fi
    fi
fi

status_code=$(jq -n \
    '{}' \
    | curl -X GET \
    --header "PRIVATE-TOKEN: ${PRIVATE_TOKEN}" \
    --header "Content-Type: application/json" \
    -d @- \
    --silent \
    --write-out %{http_code} \
    --output ${CURL_RESPONSE} \
    "https://git.code.oa.com/api/v3/projects/${source_project_id}")

if [[ ${status_code} -ne 200 ]]; then
    printf "FAILED\n"

    response=$(cat ${CURL_RESPONSE} | jq -r ".message")
    abort "Could not retrieve the source repo. ${response}." ${status_code}
fi

source_repo_url=$(cat ${CURL_RESPONSE} | jq -r ".http_url_to_repo")
source_repo_name="source_repo"
remote_repos=($(git remote))
source_repo_exist=0

for item in ${remote_repos[@]}
do
    if [[ ${item} == ${source_repo_name} ]]; then
        source_repo_exist=1
    fi 
done

if [[ ${source_repo_exist} -eq 0 ]]; then
    git remote add ${source_repo_name} ${source_repo_url}
fi

git fetch ${source_repo_name}

pr_branch_name="${source_branch}_MERGE_PR_${pr_num}"
git branch ${pr_branch_name} ${source_repo_name}/${source_branch} --quiet


git fetch ${PUSH_REMOTE_NAME} --quiet
if [[ $? -ne 0 ]]; then
    echo "ERROR: Could not fetch the remote repository (errno: $?)."
    exit 1
fi

source_branch_head=$(git rev-parse --verify --quiet ${pr_branch_name})
if [[ -z ${source_branch_head} ]]; then
    echo "ERROR: The source branch (${pr_branch_name}) does not exists in the repository."
    exit 1
fi

target_branch_head=$(git rev-parse --verify --quiet ${PUSH_REMOTE_NAME}/${target_branch})
if [[ -z ${target_branch_head} ]]; then
    echo "ERROR: The target branch (${target_branch}) does not exists in the repository."
    exit 1
fi

missing_commits=( $(git rev-list ${PUSH_REMOTE_NAME}/${target_branch} ^${pr_branch_name}) )
if (( ${#missing_commits[@]} )); then
    echo "ERROR: The source branch diverges from the target branch."
    exit 1
fi

submitting_commits=( $(git rev-list ${pr_branch_name} ^${PUSH_REMOTE_NAME}/${target_branch}) )
if [[ ${#submitting_commits[@]} -eq 0 ]]; then
    echo "ERROR: There is not commit to merge."
    exit 1
fi

echo "The following commit will be submitted:"
for change in ${submitting_commits[@]}
do 
    git log --format="%n  %s%n  Author: %aN <%aE>%n  Date: %aD%n" -n 1 ${change}
done

read -p "Do you want to continue? [Y/n] " continue
if [[ ${continue} != "Y" && ${continue} != "y" ]]; then
    exit 0
fi

# Save the current working directory
old_branch=$(git branch | grep '^*' | sed 's/* //' )

uncommitted_changes=( $(git diff HEAD --name-status) )
if (( ${#uncommitted_changes[@]} )); then
    git stash --quiet
    if [[ $? -ne 0 ]]; then
        echo "ERROR: Could not save uncommitted changes in the working directory."
        exit 1
    fi

    has_stashed_changes="true"
else
    has_stashed_changes="false"
fi

# Create a local target branch
local_target_branch="merge-${target_branch_head}"
local_target_branch_head=$(git rev-parse --verify --quiet ${PUSH_REMOTE_NAME}/${local_target_branch})
if [[ -n ${local_target_branch_head} ]]; then
    abort "The local branch (${local_target_branch}) already exists." 1
fi

printf "Checking out the target branch..."
git checkout -b ${local_target_branch} ${PUSH_REMOTE_NAME}/${target_branch} --quiet
if [[ $? -ne 0 ]]; then
    printf "FAILED\n"
    abort "Can not checkout out the target branch." $?
else
    printf "DONE\n"
fi

printf "Mering the source branch..."
git merge ${pr_branch_name} --ff-only --squash --quiet 
if [[ $? -ne 0 ]]; then
    printf "FAILED\n"
    abort "Cannot merge the source branch." $?
else
    printf "DONE\n"
fi

printf "Committing the changes to submit..."
git commit --author="${author} <${author}@tencent.com>" -m "${title}" --quiet 
if [[ $? -ne 0 ]]; then
    printf "FAILED\n"
    abort "Could not commit squashed changes" $?
else
    printf "DONE\n"
fi

printf "Pushing changes to the target branch..."
git push ${PUSH_REMOTE_NAME} ${local_target_branch}:${target_branch} >/dev/null 2>&1
if [[ $? -ne 0 ]]; then
    printf "FAILED\n"
    abort "Cannot push changes to the target branch." $?
else
    printf "DONE\n"
fi

# Close the merge request
status_code=$(jq -n \
    --arg state_event "close" \
    '{ state_event: $state_event }' \
    | curl -X PUT \
    --header "PRIVATE-TOKEN: ${PRIVATE_TOKEN}" \
    --header "Content-Type: application/json" \
    -d @- \
    --silent \
    --write-out %{http_code} \
    --output ${CURL_RESPONSE} \
    "${TARGET_REPO_REST_URL}/merge_request/${mr_id}")

if [[ ${status_code} -ne 200 ]]; then
    printf "FAILED\n"

    response=$(cat ${CURL_RESPONSE} | jq -r ".message")
    abort "Could not close the merge request. ${response}." ${status_code}
fi

cleanup