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


"""Google Cloud Dataproc Authenticator for Sparkmagic"""


import json
import os
import subprocess
import re
import random
import urllib3.util
from hdijupyterutils.ipythondisplay import IpythonDisplay
import ipyvuetify as v
from google.cloud import dataproc_v1beta2
import google.auth.transport.requests
from google.auth import _cloud_sdk
from google.auth.exceptions import UserAccessTokenError
from google.oauth2.credentials import Credentials
from sparkmagic.auth.customauth import Authenticator
from sparkmagic.livyclientlib.exceptions import BadUserConfigurationException
import googledataprocauthenticator.utils.constants as constants


ipython_display = IpythonDisplay()

def list_credentialed_accounts():
    """Load all of user's credentialed accounts with ``gcloud auth list`` command.

    Returns:
        Sequence[str]: each value is a str of one of the users credentialed accounts

    Raises:
        sparkmagic.livyclientlib.BadUserConfigurationException: if gcloud cannot be invoked
    """
    accounts_json = ""
    if os.name == "nt":
        command = constants.CLOUD_SDK_WINDOWS_COMMAND
    else:
        command = constants.CLOUD_SDK_POSIX_COMMAND
    try:
        command = (command,) + constants.CLOUD_SDK_USER_CREDENTIALED_ACCOUNTS_COMMAND
        # run `gcloud auth list` command
        accounts_json = subprocess.check_output(command, stderr=subprocess.STDOUT)
        account_objects = json.loads(accounts_json)
        credentialed_accounts = list()
        active_account = None
        #convert account dictionaries with status and account keys to a list of accounts
        for account in account_objects:
            try:
                _cloud_sdk.get_auth_access_token(account['account'])
                if account['status'] == 'ACTIVE':
                    active_account = account['account']
                credentialed_accounts.append(account['account'])
            # when`gcloud auth print-access-token --account=account` fails we don't add it to
            # credentialed_accounts dict that populates account dropdown widget
            except UserAccessTokenError:
                pass
        return credentialed_accounts, active_account
    except Exception as caught_exc:
        new_exc = BadUserConfigurationException("Gcloud cannot be invoked.")
        raise new_exc from caught_exc

def get_project_id(account):
    """Gets the the Cloud SDK project ID property value using the
    ``gcloud config get-value project --account=ACCOUNT`` command.

    Args:
        account (str): The account to get the project ID for

    Returns:
        Optional[str]: The project ID.
    """
    if os.name == "nt":
        command = constants.CLOUD_SDK_WINDOWS_COMMAND
    else:
        command = constants.CLOUD_SDK_POSIX_COMMAND

    try:
        config_get_project_command = ("config", "get-value", 'project', '--account', account)
        output = subprocess.check_output(
            (command,) + config_get_project_command, stderr=subprocess.STDOUT
        )
        return output.decode("utf-8").rstrip()
    except Exception:
        return None

def get_credentials_for_account(account, scopes_list):
    """Load all of user's credentialed accounts with ``gcloud auth describe ACCOUNT`` command.

    Args:
        account (str): user credentialed account to return credentials for
        scopes_list (Sequence[str]): list of scopes to include in the credentials.

    Returns:
        google.oauth2.credentials.Credentials: The constructed credentials

    Raises:
        ValueError: If `gcloud auth describe ACCOUNT --format json` returns json not in the
            expected format.
        google.auth.exceptions.UserAccessTokenError: if credentials could not be found for the
            given account.
    """
    if os.name == "nt":
        command = constants.CLOUD_SDK_WINDOWS_COMMAND
    else:
        command = constants.CLOUD_SDK_POSIX_COMMAND
    try:
        describe_account_command = ("auth", "describe", account, '--format', 'json')
        command = (command,) + describe_account_command
        account_json = subprocess.check_output(command, stderr=subprocess.STDOUT)
        account_describe = json.loads(account_json)
        credentials = Credentials.from_authorized_user_info(account_describe, scopes=scopes_list)
        # if quota_project_id is None, we try to get infer a project from that accounts gcloud
        # configuration
        if credentials.quota_project_id is None:
            credentials = credentials.with_quota_project(get_project_id(account))
        return (credentials, credentials.quota_project_id)
    except Exception as caught_exc:
        new_exc = UserAccessTokenError(f"Could not obtain access token for {account}")
        raise new_exc from caught_exc

def get_component_gateway_url(project_id, region, cluster_name, credentials):
    """Gets the component gateway url for a cluster name, project id, and region

    Args:
        project_id (str): The project id to use for the url
        region (str): The project id to use for the url
        cluster_name (Optional[str]): The cluster name to use for the url
        credentials (google.oauth2.credentials.Credentials): The authorization credentials to
        attach to requests.

    Returns:
        str: the component gateway url

    Raises:
        google.api_core.exceptions.GoogleAPICallError: If the request failed for any reason.
        google.api_core.exceptions.RetryError: If the request failed due to a retryable error and
        retry attempts failed.
        ValueError: If the parameters are invalid.
    """
    try:
        client = dataproc_v1beta2.ClusterControllerClient(
            credentials=credentials,
            client_options={
                "api_endpoint": f"{region}-dataproc.googleapis.com:443"
            }
            )
    except:
        raise
    try:
        #if they do not enter a cluster name, we get a random one for them.
        if cluster_name is None:
            cluster_pool, _ = get_cluster_pool(project_id, region, client)
            cluster_name = random.choice(cluster_pool)
        response = client.get_cluster(project_id, region, cluster_name)
        url = response.config.endpoint_config.http_ports.popitem()[1]
        parsed_uri = urllib3.util.parse_url(url)
        endpoint_address = f"{parsed_uri.scheme}://{parsed_uri.netloc}/gateway/default/livy/v1"
        return endpoint_address, cluster_name
    except:
        raise

def get_cluster_pool(project_id, region, client, selected_filters=None):
    """Gets the clusters for a project, region, and filters

    Args:
        project_id (str): The project id to use
        region (str): The region to use
        client (dataproc_v1beta2.ClusterControllerClient): The client that provides the
        listing clusters method
        credentials (google.oauth2.credentials.Credentials): The authorization credentials to
        attach to requests.

    Returns:
        str: the component gateway url

    Raises:
        google.api_core.exceptions.GoogleAPICallError: If the request failed for any reason.
        google.api_core.exceptions.RetryError: If the request failed due to a retryable error and
        retry attempts failed.
        ValueError: If the parameters are invalid.
    """
    cluster_pool = list()
    filter_set = set()
    filters = ['status.state=ACTIVE']
    if selected_filters is not None:
        filters.extend(selected_filters)
    filter_str = ' AND '.join(filters)
    try:
        for cluster in client.list_clusters(project_id, region, filter_str):
            #check component gateway is enabled
            if len(cluster.config.endpoint_config.http_ports.values()) != 0:
                action_list = list()
                for action in cluster.config.initialization_actions:
                    # check if livy init action with a region with the regex pattern [a-z0-9-]+
                    is_livy_action = re.search("gs://goog-dataproc-initialization-actions-"\
                    "[a-z0-9-]+/livy/livy.sh", action.executable_file) is not None
                    if is_livy_action:
                        action_list.append(action.executable_file)
                        cluster_pool.append(cluster.cluster_name)
                        for key, value in cluster.labels.items():
                            filter_set.add('labels.' + key + '=' + value)
        return cluster_pool, list(filter_set)
    except:
        raise

def get_regions():
    """Returns a static list of regions for the region combobox"""
    regions = ['asia-east1', 'asia-east2', 'asia-northeast1', 'asia-northeast2', 'asia-northeast3',\
    'asia-south1', 'asia-southeast1', 'asia-southeast2', 'australia-southeast1', 'europe-north1', \
    'europe-west1', 'europe-west2', 'europe-west3', 'europe-west4', 'europe-west5', 'europe-west6',\
    'northamerica-northeast1', 'southamerica-east1', 'us-central1', 'us-central2', 'us-east1', \
    'us-east2', 'us-east4', 'us-west1', 'us-west2', 'us-west3', 'us-west4']
    return regions

def application_default_credentials_configured():
    """Checks if google application-default credentials are configured"""
    callable_request = google.auth.transport.requests.Request()
    try:
        credentials, _ = google.auth.default(scopes=['https://www.googleapis.com/auth/' \
        'cloud-platform', 'https://www.googleapis.com/auth/userinfo.email'])
        credentials.refresh(callable_request)
    except:
        return False
    return credentials is not None


class GoogleAuth(Authenticator):
    """Custom Authenticator to use Google OAuth with SparkMagic."""

    def __init__(self, parsed_attributes=None):
        self.callable_request = google.auth.transport.requests.Request()
        self.scopes = ['https://www.googleapis.com/auth/cloud-platform',
                       'https://www.googleapis.com/auth/userinfo.email']
        self.credentialed_accounts, active_user_account = list_credentialed_accounts()
        self.default_credentials_configured = application_default_credentials_configured()
        if self.default_credentials_configured:
            self.credentialed_accounts.append('default-credentials')
        self.active_credentials = None
        if parsed_attributes is not None:
            if parsed_attributes.account in self.credentialed_accounts:
                self.active_credentials = parsed_attributes.account
                if self.active_credentials == 'default-credentials' and \
                self.default_credentials_configured:
                    self.credentials, self.project = google.auth.default(scopes=self.scopes)
                else:
                    self.credentials, self.project = get_credentials_for_account(\
                        self.active_credentials, self.scopes)
            else:
                new_exc = BadUserConfigurationException(
                    f"{parsed_attributes.account} is not a credentialed account. Run `gcloud "\
                "auth login` in your command line to authorize gcloud to access the Cloud "\
                "Platform with Google user credentials to authenticate. Run `gcloud auth "\
                "application-default login` to acquire new user credentials to use for "\
                "Application Default Credentials. Run `gcloud auth list` to see your credentialed "\
                "accounts.")
                raise new_exc
        else:
            if self.default_credentials_configured:
                self.credentials, self.project = google.auth.default(scopes=self.scopes)
                self.active_credentials = 'default-credentials'
            elif active_user_account is not None:
                self.credentials, self.project = get_credentials_for_account(
                    active_user_account, self.scopes)
                self.active_credentials = active_user_account
            else:
                self.credentials, self.project = None, None
        Authenticator.__init__(self, parsed_attributes)
        self.widgets = self.get_widgets(constants.WIDGET_WIDTH)

    def get_widgets(self, widget_width):
        """Creates and returns an address widget

        Args:
            widget_width (str): The width of all widgets to be created.

        Returns:
            Sequence[hdijupyterutils.ipywidgetfactory.IpyWidgetFactory]: list of widgets
        """
        self.project_widget = v.TextField(
            class_='ma-2',
            placeholder=constants.ENTER_PROJECT_MESSAGE,
            label='Project ID *',
            dense=True,
            v_model=self.project, #will be none if no project can be determined from credentials
            color='primary',
            outlined=True,
        )

        self.account_widget = v.Select(
            class_='ma-2',
            placeholder='No accounts found',
            label='Account *',
            dense=True,
            color='primary',
            hide_selected=False,
            outlined=True,
            v_model=self.active_credentials,
            items=self.credentialed_accounts,
            auto_select_first=True,
            # v_slots allows help message to be displayed if no accounts are found.
            v_slots=[{
                'name':
                'no-data',
                'children':
                v.ListItem(children=[
                    v.ListItemContent(children=[
                        v.ListItemTitle(
                            children=[constants.NO_ACCOUNTS_FOUND_HELP_MESSAGE])
                    ])
                ])
            }],
        )

        self.region_widget = v.Combobox(
            class_='ma-2',
            placeholder=constants.SELECT_REGION_MESSAGE,
            label='Region *',
            dense=True,
            color='primary',
            hide_selected=True,
            outlined=True,
            items=get_regions(),
            v_model=None,
        )

        self.filter_widget = v.Combobox(
            class_='ma-2',
            placeholder=constants.NO_FILTERS_FOUND_MESSAGE,
            multiple=True,
            label='Filter by label',
            chips=True,
            dense=True,
            deletable_chips=True,
            color='primary',
            hide_selected=True,
            outlined=True,
            items=[],
            auto_select_first=True,
            v_model=None,
            v_slots=[{
                'name':
                'no-data',
                'children':
                v.ListItem(children=[
                    v.ListItemContent(children=[
                        v.ListItemTitle(
                            children=[constants.NO_FILTERS_FOUND_HELP_MESSAGE])
                    ])
                ])
            }],
        )

        self.cluster_widget = v.Combobox(
            class_='ma-2',
            placeholder=constants.NO_CLUSTERS_FOUND_MESSAGE,
            label='Cluster',
            dense=True,
            color='primary',
            hide_selected=True,
            outlined=True,
            items=[],
            auto_select_first=True,
            v_model=None,
            v_slots=[{
                'name':
                'no-data',
                'children':
                v.ListItem(children=[
                    v.ListItemContent(children=[
                        v.ListItemTitle(
                            children=[constants.NO_CLUSTERS_FOUND_HELP_MESSAGE])
                    ])
                ])
            }],
        )

        self.account_widget.on_event('change', self._update_active_credentials)
        self.project_widget.on_event('change', self._update_project)
        self.region_widget.on_event('change', self._update_cluster_list_on_region)
        self.filter_widget.on_event('change', self._update_cluster_list_on_filter)
        widgets = [self.account_widget, self.project_widget, self.region_widget,
                   self.cluster_widget, self.filter_widget]
        return widgets

    def _update_project(self, _widget, _event, data):
        if self.account_widget.v_model is not None and self.region_widget.v_model is not None:
            self.initialize_credentials_with_auth_account_selection(self.account_widget.v_model)
            # checks if project texbox's value is valid
            try:
                _, _ = get_component_gateway_url(data, self.region_widget.v_model, None,
                                                 self.credentials)
                self.project_widget.error = False
                self.region_widget.error = False
                self.project = self.project_widget.v_model
                client = dataproc_v1beta2.ClusterControllerClient(
                    credentials=self.credentials,
                    client_options={
                        "api_endpoint": f"{self.region_widget.v_model}-dataproc.googleapis.com:443"
                    }
                    )
                self.cluster_widget.items, self.filter_widget.items = get_cluster_pool(
                    self.project_widget.v_model, self.region_widget.v_model, client
                )
                self._update_widgets_placeholder_text()
            except IndexError:
                self.project_widget.error = False
                pass
            except Exception:
                self.project_widget.error = True
                ipython_display.send_error("Please make sure you have entered a correct Project "\
                    "ID and Region.")
                self.cluster_widget.placeholder = constants.NO_CLUSTERS_FOUND_MESSAGE
                self.filter_widget.placeholder = constants.NO_FILTERS_FOUND_MESSAGE
                self.cluster_widget.items = []
                self.filter_widget.items = []

    def _update_active_credentials(self, _widget, _event, data):
        self.initialize_credentials_with_auth_account_selection(data)
        self.active_credentials = data
        self.project_widget.error = False
        self.region_widget.error = False
        if self.project_widget.v_model != self.project:
            self.project_widget.v_model = self.project
        self.region_widget.v_model = None
        self.cluster_widget.items = []
        self.filter_widget.items = []
        self._update_widgets_placeholder_text()

    def _update_cluster_list_on_region(self, _widget, _event, data):
        if self.account_widget.v_model is not None and self.project_widget.v_model is not None:
            try:
                _, _ = get_component_gateway_url(self.project_widget.v_model, data, None,
                                                 self.credentials)
                self.region_widget.error = False
                self.project_widget.error = False
                client = dataproc_v1beta2.ClusterControllerClient(
                    credentials=self.credentials,
                    client_options={
                        "api_endpoint": f"{data}-dataproc.googleapis.com:443"
                    }
                    )
                self.cluster_widget.items, self.filter_widget.items = get_cluster_pool(
                    self.credentials.quota_project_id, data, client
                )
                self._update_widgets_placeholder_text()
            except IndexError:
                self.region_widget.error = False
                pass
            except:
                self.region_widget.error = True
                ipython_display.send_error("Please make sure you have entered a correct Project "\
                    "ID and Region.")
                self.cluster_widget.placeholder = constants.NO_CLUSTERS_FOUND_MESSAGE
                self.filter_widget.placeholder = constants.NO_FILTERS_FOUND_MESSAGE
                self.cluster_widget.items = []
                self.filter_widget.items = []

    def _update_cluster_list_on_filter(self, _widget, _event, data):
        self.initialize_credentials_with_auth_account_selection(self.account_widget.v_model)
        if self.project_widget.v_model != self.project and self.project is not None:
            self.project_widget.v_model = self.project
        #we need to update filters and clusters now
        if self.region_widget.v_model is not None:
            try:
                client = dataproc_v1beta2.ClusterControllerClient(
                    credentials=self.credentials,
                    client_options={
                        "api_endpoint": f"{self.region_widget.v_model}-dataproc.googleapis.com:443"
                    }
                    )
                #we update cluster dropdown
                self.cluster_widget.items, _ = get_cluster_pool(
                    self.project_widget.v_model, self.region_widget.v_model, client, data
                )
                self._update_widgets_placeholder_text()
            except Exception as caught_exc:
                self.cluster_widget.placeholder = constants.NO_CLUSTERS_FOUND_MESSAGE
                self.filter_widget.placeholder = constants.NO_FILTERS_FOUND_MESSAGE
                ipython_display.send_error(f"Failed to create a client with the api_endpoint: "\
                    f"{self.region_widget.v_model}-dataproc.googleapis.com:443 due to an error: "\
                    f"{str(caught_exc)}")

    def _update_widgets_placeholder_text(self):
        """Helper method to update the cluster and filters placeholder text"""
        if len(self.cluster_widget.items) != 0:
            self.cluster_widget.placeholder = constants.SELECT_CLUSTER_MESSAGE
        else:
            self.cluster_widget.placeholder = constants.NO_CLUSTERS_FOUND_MESSAGE
        if len(self.filter_widget.items) != 0:
            self.filter_widget.placeholder = constants.SELECT_FILTER_MESSAGE
        else:
            self.filter_widget.placeholder = constants.NO_FILTERS_FOUND_MESSAGE

    def initialize_credentials_with_auth_account_selection(self, account):
        """Initializes self.credentials with the accound selected from the auth dropdown widget"""
        if account != self.active_credentials:
            if account == 'default-credentials':
                self.credentials, self.project = google.auth.default(scopes=self.scopes)
            else:
                self.credentials, self.project = get_credentials_for_account(account, self.scopes)

    def update_with_widget_values(self):
        """Updates url to be the component gateway url of the cluster found with the project,
        region, and cluster textbox widget values"""
        no_credentials_exception = BadUserConfigurationException(
            "Failed to obtain access token. Run `gcloud auth login` in your command line "\
            "to authorize gcloud to access the Cloud Platform with Google user credentials to "\
            "authenticate. Run `gcloud auth application-default login` acquire new user "\
            "credentials to use for Application Default Credentials.")
        if self.credentials is not None:
            try:
                self.initialize_credentials_with_auth_account_selection(self.account_widget.v_model)
                self.url, self.cluster_widget.v_model = get_component_gateway_url(
                    self.project_widget.v_model, self.region_widget.v_model,
                    self.cluster_widget.v_model, self.credentials
                )
            except:
                raise
        else:
            raise no_credentials_exception

    def __call__(self, request):
        if not self.credentials.valid:
            self.credentials.refresh(self.callable_request)
        request.headers['Authorization'] = f'Bearer {self.credentials.token}'
        return request

    def __hash__(self):
        return hash((self.active_credentials, self.url, self.__class__.__name__))
