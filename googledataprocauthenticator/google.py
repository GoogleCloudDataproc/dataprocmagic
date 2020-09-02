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
from hdijupyterutils.ipywidgetfactory import IpyWidgetFactory
import ipywidgets
import ipyvuetify as v
from google.cloud import dataproc_v1beta2
import google.auth.transport.requests
from google.auth import _cloud_sdk
from google.auth.exceptions import UserAccessTokenError
from google.oauth2.credentials import Credentials
from sparkmagic.auth.customauth import Authenticator
from sparkmagic.livyclientlib.exceptions import BadUserConfigurationException
from sparkmagic.utils.constants import WIDGET_WIDTH

_ENTER_PROJECT_MESSAGE = "Enter a project ID"

_SELECT_REGION_MESSAGE = "Select a region"
_NO_ACCOUNTS_FOUND_MESSAGE = "No accounts found"
_NO_ACCOUNTS_FOUND_HELP_MESSAGE = "Run `gcloud auth login` in "\
    "your command line to authorize gcloud to access the Cloud Platform with Google user "\
    "credentials to authenticate. Run `gcloud auth application-default login` acquire new "\
    "user credentials to use for Application Default Credentials. Run `gcloud auth list` to see "\
    "your credentialed accounts."
_SELECT_CLUSTER_MESSAGE = "Select a cluster"
_NO_CLUSTERS_FOUND_MESSAGE = "No clusters found"
_NO_CLUSTERS_FOUND_HELP_MESSAGE = f"{_NO_CLUSTERS_FOUND_MESSAGE}. Please make sure you have the correct "\
    "account, project ID, and region."
_SELECT_FILTER_MESSAGE = "Select a filter"
_NO_FILTERS_FOUND_MESSAGE = "No filters found"
_NO_FILTERS_FOUND_HELP_MESSAGE = f"{_NO_FILTERS_FOUND_MESSAGE}. Please make sure you have the correct "\
    "account, project ID, and region."
# The name of the Cloud SDK shell script
_CLOUD_SDK_POSIX_COMMAND = "gcloud"
_CLOUD_SDK_WINDOWS_COMMAND = "gcloud.cmd"
# The command to get all credentialed accounts 
_CLOUD_SDK_USER_CREDENTIALED_ACCOUNTS_COMMAND = ("auth", "list", "--format", "json")

def list_accounts_pairs(credentialed_accounts, default_credentials_configured):
    """Reformats all of user's credentialed accounts to populate google_credentials_widget
    dropdown's options. 

    Args:
        credentialed_accounts (str): user credentialed account to return credentials for
        default_credentials_configured (boolean): This is True if google application-default
        credentials are configured.

    Returns:
        dict: each key is a str of the users credentialed accounts and it maps to the 
        same str credentialed account
    """
    accounts = credentialed_accounts
    accounts_dict = {}
    for account in accounts:
        accounts_dict[account] = account
    if default_credentials_configured:
        accounts_dict['default-credentials'] = 'default-credentials'
    return accounts_dict

def list_credentialed_accounts():
    """Load all of user's credentialed accounts with ``gcloud auth list`` command.

    Returns:
        Sequence[str]: each key is a str of one of the users credentialed accounts

    Raises:
        sparkmagic.livyclientlib.BadUserConfigurationException: if gcloud cannot be invoked
    """
    accounts_json = ""
    if os.name == "nt":
        command = _CLOUD_SDK_WINDOWS_COMMAND
    else:
        command = _CLOUD_SDK_POSIX_COMMAND
    try:
        command = (command,) + _CLOUD_SDK_USER_CREDENTIALED_ACCOUNTS_COMMAND
        # run `gcloud auth list` command 
        accounts_json = subprocess.check_output(command, stderr=subprocess.STDOUT)
        all_accounts = json.loads(accounts_json)
        credentialed_accounts = set()
        active_account = None
        for account in all_accounts:
            try:
                _cloud_sdk.get_auth_access_token(account['account'])
                if account['status'] == 'ACTIVE':
                    active_account = account['account']
                credentialed_accounts.add(account['account'])
            # when`gcloud auth print-access-token --account=account` fails we don't add it to
            # credentialed_accounts dict that populates account dropdown widget
            except UserAccessTokenError:
                pass
        return credentialed_accounts, active_account
    except Exception as caught_exc:
        new_exc = BadUserConfigurationException("Gcloud cannot be invoked.")
        raise new_exc from caught_exc

def get_credentials_for_account(account, scopes_list):
    """Load all of user's credentialed accounts with ``gcloud auth describe ACCOUNT`` command.

    Args:
        account (str): user credentialed account to return credentials for
        scopes_list (Sequence[str]): list of scopes to include in the credentials.
    
    Returns:
        google.oauth2.credentials.Credentials: The constructed credentials

    Raises:
        ValueError: If `gcloud auth describe ACCOUNT --format json` returns json not in the expected format.
        google.auth.exceptions.UserAccessTokenError: if credentials could not be found for the given account. 
    """
    if os.name == "nt":
        command = _CLOUD_SDK_WINDOWS_COMMAND
    else:
        command = _CLOUD_SDK_POSIX_COMMAND
    try:
        describe_account_command = ("auth", "describe", account, '--format', 'json')
        command = (command,) + describe_account_command
        account_json = subprocess.check_output(command, stderr=subprocess.STDOUT)
        account_describe = json.loads(account_json)
        credentials = Credentials.from_authorized_user_info(account_describe, scopes=scopes_list)
        return (credentials, credentials.quota_project_id)
    except Exception as caught_exc:
        new_exc = UserAccessTokenError(f"Could not obtain access token for {account}")
        raise new_exc from caught_exc

def get_component_gateway_url(project_id, region, cluster_name, credentials):
    """Gets the component gateway url for a cluster name, project id, and region

    Args:
        cluster_name (str): The cluster name to use for the url
        project_id (str): The project id to use for the url
        region (str): The project id to use for the url
        credentials (google.oauth2.credentials.Credentials): The authorization credentials to
        attach to requests.

    Returns:
        str: the component gateway url

    Raises:
        google.api_core.exceptions.GoogleAPICallError: If the request failed for any reason.
        google.api_core.exceptions.RetryError: If the request failed due to a retryable error and retry attempts failed.
        ValueError: If the parameters are invalid.
    """
    client = dataproc_v1beta2.ClusterControllerClient(credentials=credentials,
                       client_options={
                            "api_endpoint": f"{region}-dataproc.googleapis.com:443"
                        }
                    )
    try:
        #if they do not enter a cluster name, we get a random one for them.
        if cluster_name is None:
            cluster_name = random.choice(get_cluster_pool(project_id, region, client))
        response = client.get_cluster(project_id, region, cluster_name)
        url = response.config.endpoint_config.http_ports.popitem()[1]
        parsed_uri = urllib3.util.parse_url(url)
        endpoint_address = f"{parsed_uri.scheme}://{parsed_uri.netloc}/" + "gateway/default/livy/v1"
        return endpoint_address
    except:
        raise

def get_cluster_pool(project_id, region, client, selected_filters=None):
    cluster_pool = list()
    filter_set = set()
    filters = ['status.state = ACTIVE']
    if selected_filters is not None:
        filters.extend(selected_filters)
    #filter format: status.state = ACTIVE AND clusterName = mycluster AND labels.env = staging AND labels.starred = \*
    filter_str = ' AND '.join(filters)
    print(filter_str)
    try:
        for cluster in client.list_clusters(project_id, region, filter_str):
            #check component gateway is enabled
            if (len(cluster.config.endpoint_config.http_ports.values()) != 0):
                action_list = list()
                for action in cluster.config.initialization_actions:
                    #check if action is livy init action with a region with regex pattern [a-z0-9-]+
                    is_livy_action = re.search("gs://goog-dataproc-initialization-actions-[a-z0-9-]+/livy/livy.sh", action.executable_file) is not None
                    if is_livy_action:
                        action_list.append(action.executable_file)
                        print(cluster.cluster_name)
                        cluster_pool.append(cluster.cluster_name)
                        print(cluster.labels)
                        for _filter in cluster.labels:
                            filter_set.add(_filter)
                            print(filter_set)
        print(list(filter_set))
        print(cluster_pool)
        return cluster_pool, list(filter_set)
    except: 
        raise

# def get_filter_labels(project_id, region, client, filters=None):
#     if os.name == "nt":
#         command = _CLOUD_SDK_WINDOWS_COMMAND
#     else:
#         command = _CLOUD_SDK_POSIX_COMMAND
#     try:
#         output = subprocess.check_output(
#             [command, "dataproc", "clusters", "list", f"--project={project_id}", f"--region={region}" "--format=json"]
#         )
#         print(output)
#         print(type(output))
#         print(dir(output))
#         data = json.loads(output.decode("utf-8"))
#         #returns list of region names
#         labels_list = [_["labels"] for _ in data]
#         clusters_list = [_["labels"] for _ in data]
#         return set(labels_list)
#     except Exception:
#         return set()
    

def get_regions(project=None):
    regions = ['asia-east1', 'asia-east2', 'asia-northeast1', 'asia-northeast2', 'asia-northeast3',
    'asia-south1', 'asia-southeast1', 'asia-southeast2', 'australia-southeast1', 'europe-north1', 
    'europe-west1', 'europe-west2', 'europe-west3', 'europe-west4', 'europe-west5', 'europe-west6', 
    'northamerica-northeast1', 'southamerica-east1', 'us-central1', 'us-central2', 'us-east1', 
    'us-east2', 'us-east4', 'us-west1', 'us-west2', 'us-west3', 'us-west4']
    return regions
    # if os.name == "nt":
    #     command = _CLOUD_SDK_WINDOWS_COMMAND
    # else:
    #     command = _CLOUD_SDK_POSIX_COMMAND
    # try:
    #     output = subprocess.check_output(
    #         [command, "compute", "regions", "list", "--project", project, "--format=json"]
    #     )
    #     data = json.loads(output.decode("utf-8"))
    #     #returns list of region names
    #     return [_["name"] for _ in data]
    # except Exception:
    #     return []

def application_default_credentials_configured():
    """Checks if google application-default credentials are configured"""
    try:
        credentials, _ = google.auth.default(scopes=['https://www.googleapis.com/auth/cloud-platform', 'https://www.googleapis.com/auth/userinfo.email'])
    except:
        return False
    return credentials is not None


class GoogleAuth(Authenticator):
    """Custom Authenticator to use Google OAuth with SparkMagic."""

    def __init__(self, parsed_attributes=None):
        self.callable_request = google.auth.transport.requests.Request()
        self.credentialed_accounts, active_user_account = list_credentialed_accounts()
        self.scopes = ['https://www.googleapis.com/auth/cloud-platform', 'https://www.googleapis.com/auth/userinfo.email']
        self.active_credentials = None
        self.default_credentials_configured = application_default_credentials_configured()
        account_dict = list_accounts_pairs(self.credentialed_accounts, self.default_credentials_configured)
        self.client = None
        if parsed_attributes is not None:
            if parsed_attributes.account in account_dict:
                self.active_credentials = parsed_attributes.account
                if self.active_credentials == 'default-credentials' and self.default_credentials_configured:
                    self.credentials, self.project = google.auth.default(scopes=self.scopes)
                else:
                    self.credentials, self.project = get_credentials_for_account(self.active_credentials, self.scopes)
            else:
                new_exc = BadUserConfigurationException(
                f"{parsed_attributes.account} is not a credentialed account. Run `gcloud auth login` in "\
                "your command line to authorize gcloud to access the Cloud Platform with Google user "\
                "credentials to authenticate. Run `gcloud auth application-default login` acquire new "\
                "user credentials to use for Application Default Credentials. Run `gcloud auth list` to see "\
                "your credentialed accounts.")
                raise new_exc
        else:
            if self.default_credentials_configured:
                self.credentials, self.project = google.auth.default(scopes=self.scopes)
                self.active_credentials = 'default-credentials'
            elif active_user_account is not None:
                self.credentials, self.project = get_credentials_for_account(active_user_account, self.scopes)

                self.active_credentials = active_user_account
            else:
                self.credentials, self.project = None, None
        Authenticator.__init__(self, parsed_attributes)
        self.widgets = self.get_widgets(WIDGET_WIDTH)

    def get_widgets(self, widget_width):
        """Creates and returns an address widget

        Args:
            widget_width (str): The width of all widgets to be created.

        Returns:
            Sequence[hdijupyterutils.ipywidgetfactory.IpyWidgetFactory]: list of widgets
        """
        ipywidget_factory = IpyWidgetFactory()

        # self.project_widget = ipywidget_factory.get_text(
        #     description='Project:',
        #     width=widget_width
        # )

        self.project_textfield = v.TextField(
            class_='ma-2',
            placeholder=_ENTER_PROJECT_MESSAGE,
            label='Project *',
            dense=True,
            v_model=self.project, #will be none if no project can be determined from credentials
            color='primary',
            outlined=True,
        )

        # if self.project is not None:
        #     self.project_textfield.v_model = self.project

        # self.cluster_name_widget = ipywidget_factory.get_text(
        #     description='Cluster:',
        #     width=widget_width
        # )

        # self.region_widget = ipywidget_factory.get_text(
        #     description='Region:',
        #     width=widget_width
        # )

        # self.region_dropdown = ipywidgets.Combobox(
        #     description='Region:',
        #     placeholder='Select a region',
        #     options=get_regions(),
        #     ensure_option=False
        # )

        # self.filter_by_label = ipywidgets.Combobox(
        #     placeholder=_NO_FILTERS_FOUND_MESSAGE,
        #     options=[_NO_FILTERS_FOUND_HELP_MESSAGE],
        #     disabled=True,
        #     value=None,
        #     description=u"Filter by label:"
        # )

        # self.google_credentials_widget = ipywidgets.Combobox(
        #     options=list((list_accounts_pairs(self.credentialed_accounts, self.default_credentials_configured)).keys()),
        #     value=None,
        #     description=u"Account:",
        #     ensure_option=True
        # )

        self.account_combobox = v.Combobox(
            class_='ma-2',
            placeholder='No accounts found',
            label='Account *',
            dense=True,
            color='primary',
            hide_selected=True,
            outlined=True,
            v_model=self.active_credentials, 
            items=list((list_accounts_pairs(self.credentialed_accounts, self.default_credentials_configured)).keys()),
            auto_select_first=True,
            # v_slots allows help message to be displayed if no accounts are found. 
            v_slots=[{
                'name':
                'no-data',
                'children':
                v.ListItem(children=[
                    v.ListItemContent(children=[
                        v.ListItemTitle(
                            children=[_NO_ACCOUNTS_FOUND_HELP_MESSAGE])
                    ])
                ])
            }],
        )

        # self.google_credentials_widget = ipywidget_factory.get_dropdown(
        #     options=list_accounts_pairs(self.credentialed_accounts, self.default_credentials_configured),
        #     value=None,
        #     description=u"Account:"
        # )
        self.region_combobox = v.Combobox(
            class_='ma-2',
            placeholder=_SELECT_REGION_MESSAGE,
            label='Region *',
            deletable_chips=True,
            dense=True,
            color='primary',
            hide_selected=True,
            outlined=True,
            items=get_regions(),
            v_model=None,
        )
        

        self.filter_combobox = v.Combobox(
            class_='ma-2',
            placeholder=_NO_FILTERS_FOUND_MESSAGE,
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
                            children=[_NO_FILTERS_FOUND_HELP_MESSAGE])
                    ])
                ])
            }],
        )

        self.cluster_combobox = v.Combobox(
            class_='ma-2',
            placeholder=_NO_CLUSTERS_FOUND_MESSAGE,
            label='Cluster',
            deletable_chips=True,
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
                            children=[_NO_CLUSTERS_FOUND_HELP_MESSAGE])
                    ])
                ])
            }],
        )

        # self.cluster_dropdown = ipywidgets.Combobox(
        #     placeholder=_NO_CLUSTERS_FOUND_MESSAGE,
        #     options=[_NO_CLUSTERS_FOUND_HELP_MESSAGE],
        #     description='Cluster:',
        #     ensure_option=True, 
        #     disabled=True
        # )
        
        #can comment this then say like uncomment it to populate based on project? 
        #populate region dropdown when a project is entered  

        #self.project_widget.observe(self._update_region_list)
        #populate cluster dropdown when a region is selected
        #self.region_dropdown.observe(self._update_cluster_list)
        self.account_combobox.on_event('change', self._update_active_credentials)
        self.project_textfield.on_event('change', self._update_region_list)
        self.region_combobox.on_event('change', self._update_cluster_list)
        self.filter_combobox.on_event('change', self._update_cluster_list)
        self.cluster_combobox.on_event('change', self._update_cluster_selection)

        # if self.active_credentials is not None:
        #     self.account_combobox.value = self.active_credentials
           
        # widgets = [self.project_widget, self.region_widget, self.cluster_name_widget, self.region_dropdown, self.cluster_dropdown, self.google_credentials_widget]
        widgets = [self.account_combobox, self.project_textfield, self.region_combobox, self.cluster_combobox, self.filter_combobox]
        return widgets

    def _update_cluster_selection(self, widget, event, data):
            print(data)
            self.cluster_selection = data
    
    def _update_active_credentials(self, widget, event, data):
            print(data)
            self.active_credentials = data

    def _update_region_list(self, widget, event, data):
        print(get_regions())
        self.project = data
        #self._update_region_list.items = get_regions()
    
    def _update_cluster_list(self, widget, event, data):
        print(data)
        
        #if a region is selected. 
        if widget.label == 'Region *' and data is not '' and data is not None:
            #region = change['new']
            print(data)
            print(event)
            #what error if the region is not valid? 
            try:
                self.client = dataproc_v1beta2.ClusterControllerClient(credentials=self.credentials,
                            client_options={
                                "api_endpoint": f"{data}-dataproc.googleapis.com:443"
                            }
                        )
                print(self.project_textfield.v_model)
                self.cluster_combobox.placeholder = _SELECT_CLUSTER_MESSAGE
                self.filter_combobox.placeholder = _SELECT_FILTER_MESSAGE
                self.cluster_combobox.items, self.filter_combobox.items = get_cluster_pool(self.project_textfield.v_model, data, self.client)
            except Exception as caught_exc:
                self.ipython_display.send_error(f"Failed to create a client with the api_endpoint: {data}-dataproc.googleapis.com:443"\
            f"due to an error: {str(caught_exc)}")
    
        if self.client is not None and widget.label == 'Filter by label' and data is not '' and data is not None:
                #self.cluster_dropdown.options, self.filter_by_label.options = get_cluster_pool(self.project_widget.value, region, client)
                _, self.cluster_combobox.items = get_cluster_pool(self.project_textfield.v_model, self.region_combobox.v_model, self.client, data)
            

    
    # def _update_cluster_list(self, change):
    #     if change['type'] == 'change' and change['name'] == 'value':
    #         region = change['new']
    #         print(region)
    #         #what error if the region is not valid? 
    #         client = dataproc_v1beta2.ClusterControllerClient(credentials=self.credentials,
    #                     client_options={
    #                         "api_endpoint": f"{region}-dataproc.googleapis.com:443"
    #                     }
    #                 )
    #         print(self.project_widget.value)
    #         #is
    #         #self.cluster_dropdown.placeholder = _SELECT_CLUSTER_MESSAGE
    #         self.cluster_combobox.placeholder = _SELECT_CLUSTER_MESSAGE
    #         self.filter_combobox.placeholder = _SELECT_FILTER_MESSAGE
    #         #self.filter_by_label.placeholder = _SELECT_FILTER_MESSAGE
    #         #
    #         #print(get_cluster_pool(self.project_widget.value, region, client))
    #         if self.filter_by_label.value is not None:
    #             #self.cluster_dropdown.options, self.filter_by_label.options = get_cluster_pool(self.project_widget.value, region, client)
    #             self.cluster_combobox.options, self.filter_combobox.options = get_cluster_pool(self.project_widget.value, region, client)

    #         else:
    #             _, self.cluster_combobox.options = get_cluster_pool(self.project_widget.value, region, client, self.filter_combobox.value)
            


    # def _update_filter_list(self, change):
    #     if change['type'] == 'change' and change['name'] == 'value':
    #         region = change['new']
    #         print(region)
    #         #what error if the region is not valid? 
    #         client = dataproc_v1beta2.ClusterControllerClient(credentials=self.credentials,
    #                     client_options={
    #                         "api_endpoint": f"{region}-dataproc.googleapis.com:443"
    #                     }
    #                 )
    #         print(self.project_widget.value)
    #         print(get_cluster_pool(self.project_widget.value, region, client))
            
    #         self.cluster_dropdown.options, self.filter_by_label.options = get_cluster_pool(self.project_widget.value, region, client)
            
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
                # self.url = get_component_gateway_url(self.project_widget.value, self.region_widget.value, \
                #     self.cluster_name_widget.value, self.credentials)
                self.url = get_component_gateway_url(self.project_textfield.v_model, self.region_combobox.v_model, \
                    self.cluster_combobox.v_model, self.account_combobox.v_model)
            except:
                raise
        else:
            raise no_credentials_exception
        self.initialize_credentials_with_auth_account_selection(self.active_credentials)

    def __call__(self, request):
        if not self.credentials.valid:
            self.credentials.refresh(self.callable_request)
        request.headers['Authorization'] = f'Bearer {self.credentials.token}'
        return request

    def __hash__(self):
        return hash((self.active_credentials, self.url, self.__class__.__name__))
