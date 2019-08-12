#! /usr/bin/python
# coding=utf-8

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
#  limitations under the License.
################################################################################

# Script for creating well-formed pull request,  merging and pushing them to
# https://git.code.oa.com/spark/spark
#   usage: ./merge_pr.py {pr_num}
#
# Before running script, please fill in the following configurations:
# PRIVATE_TOKEN: get it from https://git.code.oa.com/profile/account
# PUSH_REMOTE_NAME: Remote name which points to https://git.code.oa.com/spark/spark
# TARGET_PROJECT_ID: project id of the pr's target repo
# the default value 167434 is the project id of https://git.code.oa.com/spark/spark

import sys
import re
import os
import subprocess
# TODO: Support python3
import urllib2
import json

if sys.version < '3':
    input = raw_input

# get PRIVATE_TOKEN https://git.code.oa.com/profile/account
PRIVATE_TOKEN = os.environ.get("PRIVATE_TOKEN", "")

# Remote name which points to https://git.code.oa.com/spark/spark
PUSH_REMOTE_NAME = os.environ.get("PUSH_REMOTE_NAME", "")

# 165505: the project id of https://git.code.oa.com/spark/spark
TARGET_PROJECT_ID = os.environ.get("TARGET_PROJECT_ID", "167434")

TARGET_REPO_REST_URL = "https://git.code.oa.com/api/v3/projects/"

g_has_stashed_changes = False
g_old_branch = ""
g_pr_local_source_branch = ""
g_pr_local_target_branch = ""
g_pr_source_repo_name = ""

def cleanup():
    if g_old_branch != "":
        run_cmd("git checkout %s --quiet" % g_old_branch)

    if g_has_stashed_changes:
        run_cmd("git stash pop --quiet")

    if g_pr_local_source_branch != "":
        run_cmd("git branch -D %s --quiet" % g_pr_local_source_branch)

    if g_pr_local_target_branch != "":
        run_cmd("git branch -D %s --quiet" % g_pr_local_target_branch)

    if g_pr_source_repo_name != "":
        run_cmd("git remote remove " + g_pr_source_repo_name)

def err_exit():
    cleanup()
    sys.exit(-1)

def request_get(url):
    pr_heads = {
        "PRIVATE-TOKEN": PRIVATE_TOKEN,
        "Content-Type": "application/json"
    }

    try:
        request = urllib2.Request(url, headers=pr_heads)
        response = urllib2.urlopen(request)
        status_code = response.getcode()
        if status_code != 200:
            print(response)
            err_exit()
        return json.load(response)
    except urllib2.HTTPError as e:
        print("request_get url:" + url +" exception:" + str(e))
        err_exit()

def request_put(url, put_data):
    pr_heads = {
        "PRIVATE-TOKEN": PRIVATE_TOKEN,
        "Content-Type": "application/json"
    }
    put_data = json.dumps(put_data)

    try:
        request = urllib2.Request(url, headers=pr_heads, data=put_data)
        request.get_method = lambda: "PUT"
        response = urllib2.urlopen(request)
        status_code = response.getcode()
        if status_code != 200:
            print(response)
            err_exit()
    except urllib2.HTTPError as e:
        print("request_post url:" + url +" exception:" + str(e))
        err_exit()

def run_cmd(cmd):
    #print(cmd)
    if isinstance(cmd, list):
        return subprocess.check_output(cmd).strip()
    else:
        return subprocess.check_output(cmd.split(" ")).strip()

def check_env_var():
    if not PRIVATE_TOKEN:
        print("ERROR: The env-vars PRIVATE_TOKEN is not specified. You can find your token on" + \
              "https://git.code.oa.com/profile/account.")
        err_exit()

    if not PUSH_REMOTE_NAME:
        print("ERROR: The env-vars PUSH_REMOTE_NAME is not specified. ")
        err_exit()

    if not TARGET_PROJECT_ID:
        print("ERROR: The env-vars TARGET_PROJECT_ID is not specified. You can find it on" + \
              "https://git.code.oa.com/spark/spark")
        err_exit()

def get_merge_request(pr_num):
    print("Retrieving merge request %s..." % pr_num)
    merge_request_list = request_get(TARGET_REPO_REST_URL + TARGET_PROJECT_ID + "/merge_requests?iid=" + pr_num)

    if len(merge_request_list) == 0:
        print("ERROR: Cannot find the specified merge request.")
        err_exit()

    merge_request = merge_request_list[0]

    state = merge_request["state"]
    if state != "opened":
        print("ERROR: The merge request is not opened (state: %s)." % state)
        err_exit()

    status = merge_request["merge_status"]
    if status != "can_be_merged":
        print("ERROR: The merge request can not be merged (status: %s)." % status)
        err_exit()

    return merge_request

def standardize_commit_msg(merge_request):
    title = merge_request["title"]
    modified_title = title

    # Get "STORY-${id}" "BUG-${id}" "Task-${id}"
    tapd_arr = []
    pattern = re.compile(r'(story-[0-9]+)+', re.IGNORECASE)
    for ref in pattern.findall(modified_title):
        tapd_arr.append(ref)

    pattern = re.compile(r'(task-[0-9]+)+', re.IGNORECASE)
    for ref in pattern.findall(modified_title):
        tapd_arr.append(ref)

    pattern = re.compile(r'(bug-[0-9]+)+', re.IGNORECASE)
    for ref in pattern.findall(modified_title):
        tapd_arr.append(ref)

    tapd_msg = ""
    for item in tapd_arr:
        # Convert tapd msg in title from lower case to upper case
        upper_item = item.upper()
        modified_title = modified_title.replace(item, upper_item)

        # Replace '-' with '=' in tapd_msg and covert to lower
        msg = " --" + item.replace("-", "=")
        if msg in title:
            continue

        tapd_msg = tapd_msg + msg

    tapd_msg = tapd_msg.lower()

    # Get committer
    pattern = re.compile(r'(--user=[a-z]+)', re.IGNORECASE)
    for ref in pattern.findall(modified_title):
        # Remove committer from title to avoid duplicating
        modified_title = modified_title.replace(ref, "")

    # Replace multiple spaces with a single space
    modified_title = re.sub(r'\s+', ' ', title.strip()) + tapd_msg.rstrip()

    if modified_title != title:
        print("I've re-written the title as follows to match the standard format:")
        print("Original: " + title)
        print("Modified: " + modified_title)
        accept_modified_title = input("Would you like to use the modified title? (y/n): ")
        if accept_modified_title.lower() == "y":
            return modified_title

        accept_origin_title = input("Would you like to use the origin title? (y/n): ")
        if accept_origin_title.lower() != "y":
            return input("Please input new title: ").strip()

    return title

def create_source_branch(merge_request):
    global g_pr_source_repo_name
    global g_pr_local_source_branch

    pr_num = str(merge_request["iid"])
    source_project_id = str(merge_request["source_project_id"])
    source_branch = merge_request["source_branch"]

    source_repo_rsp = request_get(TARGET_REPO_REST_URL + source_project_id )
    source_repo_url = source_repo_rsp["http_url_to_repo"]

    g_pr_source_repo_name = "source_repo"
    remote_repos = run_cmd("git remote").split("\n")
    source_repo_exist = False

    for item in remote_repos:
        item = item.strip()
        if item == g_pr_source_repo_name:
            source_repo_exist = True

    try:
        if source_repo_exist == False:
            run_cmd("git remote add %s %s" % (g_pr_source_repo_name, source_repo_url))

        g_pr_local_source_branch = "source_branch_" + source_branch + "_pr_num_" + pr_num
        run_cmd("git fetch %s --quiet" % g_pr_source_repo_name)
        print("Creating the source branch...")
        run_cmd("git branch " + g_pr_local_source_branch + " " + g_pr_source_repo_name + "/" + source_branch + " --quiet")
    except Exception as e:
        print(e)
        err_exit()

def create_target_branch(merge_request):
    global g_pr_local_target_branch

    pr_num = str(merge_request["iid"])
    target_branch = merge_request["target_branch"]
    g_pr_local_target_branch = "target_branch_" + target_branch + "_pr_num_" + pr_num

    # Create a local target branch
    try:
        run_cmd("git fetch %s --quiet" % PUSH_REMOTE_NAME)
    except:
        print("ERROR: Can not fetch " + PUSH_REMOTE_NAME)
        err_exit()

    print("Creating the target branch...")
    try:
        run_cmd("git branch " + g_pr_local_target_branch + " " + PUSH_REMOTE_NAME + "/" + target_branch + " --quiet")
    except:
        print("ERROR: Could not create branch " + g_pr_local_target_branch)
        err_exit()

def check_merge():
    try:
        run_cmd("git rev-parse --verify --quiet " + g_pr_local_source_branch)
    except:
        print("ERROR: The source branch: %s does not exists in the repository." % g_pr_local_source_branch)
        err_exit()

    try:
        run_cmd("git rev-parse --verify --quiet " + g_pr_local_target_branch)
    except:
        print("ERROR: The target branch %s does not exists in the repository." % g_pr_local_target_branch)
        err_exit()

    try:
        missing_commits = run_cmd("git rev-list %s ^%s" % (g_pr_local_target_branch, g_pr_local_source_branch))
        if len(missing_commits) > 0:
            print("ERROR: The source branch diverges from the target branch.")
            err_exit()

        submitting_commits = run_cmd("git rev-list %s ^%s" % (g_pr_local_source_branch, g_pr_local_target_branch))
        if len(submitting_commits) == 0:
            print("ERROR: There is not commit to merge.")
            err_exit()

        print("The following commit will be submitted:")
        for commit in submitting_commits.split("\n"):
            print(run_cmd(['git', 'log', '--format="%n  %s%n  Author: %aN <%aE>%n  Date: %aD%n"', '-n', '1', commit]))

        continue_merge = input("Do you want to continue? (y/n): ")
        if continue_merge.lower() != "y":
            cleanup()
            sys.exit(0)
    except Exception as e:
        print(e)
        err_exit()

def save_uncommitted_changes():
    global g_old_branch
    global g_has_stashed_changes

    # Save the current working directory
    try:
        g_old_branch = run_cmd("git rev-parse --abbrev-ref HEAD")
        uncommitted_changes = run_cmd("git diff HEAD --name-status")
        if len(uncommitted_changes) > 0:
            run_cmd("git stash --quiet")
            g_has_stashed_changes = True
    except:
        print("ERROR: Could not save uncommitted changes in the working directory.")
        err_exit()

def merge_pr(merge_request):
    author = merge_request["author"]["name"]
    mr_id = str(merge_request["id"])
    target_branch = merge_request["target_branch"]
    title = standardize_commit_msg(merge_request)

    print("Checking out the target branch...")
    try:
        run_cmd("git checkout " + g_pr_local_target_branch + " --quiet")
    except:
        print("ERROR: Could not checkout branch " + g_pr_local_target_branch)
        err_exit()

    print("Mering the source branch...")
    try:
        run_cmd("git merge " + g_pr_local_source_branch + " --ff-only --squash --quiet")
    except:
        print("Cannot merge the source branch.")
        err_exit()

    print("Committing the changes to submit...")
    try:
        run_cmd(['git', 'commit', '--author=\"%s <%s@tencent.com>\"' % (author, author),  '-m %s' % title, '--quiet'])
    except:
        print("Could not commit squashed changes")
        err_exit()

    print("Pushing changes to the target branch...")
    try:
        run_cmd("git push %s %s:%s --quiet" % (PUSH_REMOTE_NAME, g_pr_local_target_branch, target_branch))
    except:
        print("Cannot push changes to the target branch.")
        err_exit()

    # close pr
    url = TARGET_REPO_REST_URL + TARGET_PROJECT_ID + "/merge_request/" + mr_id
    put_data = {"state_event" : "close"}
    request_put(url, put_data)

def main(pr_num):
    check_env_var()

    merge_request = get_merge_request(pr_num)

    create_source_branch(merge_request)
    create_target_branch(merge_request)

    check_merge()

    save_uncommitted_changes()

    merge_pr(merge_request)

    cleanup()

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("ERROR: The merge request number is not specified.")
    main(sys.argv[1])

