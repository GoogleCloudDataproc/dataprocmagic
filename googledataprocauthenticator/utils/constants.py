# Copyright 2020 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


"""Constants used by dataprocmagic"""


WIDGET_WIDTH = "1000px"
ENTER_PROJECT_MESSAGE = "Enter a project ID"
SELECT_REGION_MESSAGE = "Select a region"
SELECT_CLUSTER_MESSAGE = "Select a cluster"
SELECT_FILTER_MESSAGE = "Select a filter"

NO_ACCOUNTS_FOUND_MESSAGE = "No accounts found"
NO_CLUSTERS_FOUND_MESSAGE = "No clusters found"
NO_FILTERS_FOUND_MESSAGE = "No filters found"

NO_ACCOUNTS_FOUND_HELP_MESSAGE = "Run `gcloud auth login` in "\
    "your command line to authorize gcloud to access the Cloud Platform with Google user "\
    "credentials to authenticate. Run `gcloud auth application-default login` acquire new "\
    "user credentials to use for Application Default Credentials. Run `gcloud auth list` to see "\
    "your credentialed accounts."
NO_CLUSTERS_FOUND_HELP_MESSAGE = f"{NO_CLUSTERS_FOUND_MESSAGE}. Please make sure you have the "\
    "correct account, project ID, and region."
NO_FILTERS_FOUND_HELP_MESSAGE = f"{NO_FILTERS_FOUND_MESSAGE}. Please make sure you have the "\
    " correct account, project ID, and region."

# The name of the Cloud SDK shell script
CLOUD_SDK_POSIX_COMMAND = "gcloud"
CLOUD_SDK_WINDOWS_COMMAND = "gcloud.cmd"
# The command to get all credentialed accounts
CLOUD_SDK_USER_CREDENTIALED_ACCOUNTS_COMMAND = ("auth", "list", "--format", "json")
# The command to get all credentialed accounts
_CLOUD_SDK_CONFIG_COMMAND = ("config", "config-helper", "--format", "json")

CLOUD_SDK_USER_CREDENTIALED_ACCOUNTS_COMMAND = ("auth", "list", "--format", "json")
