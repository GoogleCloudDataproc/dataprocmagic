﻿from .customauth import Authenticator
import json
import os
import re
import urllib3.util
import subprocess
from sparkmagic.livyclientlib.exceptions import BadUserConfigurationException
from sparkmagic.utils.constants import WIDGET_WIDTH
from google.cloud import dataproc_v1beta2
import google.auth.transport.requests 
from google.auth import _cloud_sdk  
from google.auth.exceptions import DefaultCredentialsError, RefreshError
from hdijupyterutils.ipywidgetfactory import IpyWidgetFactory
from hdijupyterutils.ipythondisplay import IpythonDisplay
from google.oauth2.credentials import Credentials


# The name of the Cloud SDK shell script
_CLOUD_SDK_POSIX_COMMAND = "gcloud"
_CLOUD_SDK_WINDOWS_COMMAND = "gcloud.cmd"
# The command to get all credentialed accounts 
_CLOUD_SDK_USER_CREDENTIALED_ACCOUNTS_COMMAND = ("auth", "list", "--format", "json")

def load_json_input(result):
    """Load json data from the file."""
    jsondata = None
    try:
        jsondata = json.loads(result)
    except:
        raise
    return jsondata

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
        sparkmagic.livyclientlib.BadUserConfigurationException: if account is not set or user
        needs to run gcloud auth login or if gcloud is not installed.
    """
    accounts_json = ""
    if os.name == "nt":
        command = _CLOUD_SDK_WINDOWS_COMMAND
    else:
        command = _CLOUD_SDK_POSIX_COMMAND
    try:
        command = (command,) + _CLOUD_SDK_USER_CREDENTIALED_ACCOUNTS_COMMAND
        accounts_json = subprocess.check_output(command, stderr=subprocess.STDOUT)
        all_accounts = load_json_input(accounts_json)
        credentialed_accounts = set()
        active_account = None
        for account in all_accounts:
            try:
                _cloud_sdk.get_auth_access_token(account['account'])
                if account['status'] == 'ACTIVE':
                    active_account = account['account']
                credentialed_accounts.add(account['account'])
            except:
                pass
        return credentialed_accounts, active_account
    except (OSError) as caught_exc:
        new_exc = BadUserConfigurationException(
            "Gcloud is not installed. Install the Google Cloud SDK.")
        raise new_exc
    except (subprocess.CalledProcessError, IOError) as caught_exc:
        new_exc = BadUserConfigurationException(
            "Failed to obtain access token. Run `gcloud auth login` in your command line "\
            "to authorize gcloud to access the Cloud Platform with Google user credentials to "\
            "authenticate. Run `gcloud auth application-default login` acquire new user "\
            "credentials to use for Application Default Credentials."
        )
        raise new_exc

def get_credentials_for_account(account, scopes_list):
    """Load all of user's credentialed accounts with ``gcloud auth describe ACCOUNT`` command.

    Args:
        account (str): user credentialed account to return credentials for
        scopes_list (Sequence[str]): list of scopes to include in the credentials.
    
    Returns:
        google.oauth2.credentials.Credentials: The constructed credentials

    Raises:
        ValueError: If `gcloud auth describe ACCOUNT --format json` returns json not in the expected format.
        sparkmagic.livyclientlib.BadUserConfigurationException: if account is not set or user needs to run 
        gcloud auth login or if gcloud is not installed. 
    """
    if os.name == "nt":
        command = _CLOUD_SDK_WINDOWS_COMMAND
    else:
        command = _CLOUD_SDK_POSIX_COMMAND
    try:
        describe_account_command = ("auth", "describe", account, '--format', 'json')
        command = (command,) + describe_account_command
        account_json = subprocess.check_output(command, stderr=subprocess.STDOUT)
        account_describe = load_json_input(account_json)
        return Credentials.from_authorized_user_info(account_describe, scopes=scopes_list)
    except ValueError: 
        raise
    except (OSError) as caught_exc:
        new_exc = BadUserConfigurationException(
            "Gcloud is not installed. Install the Google Cloud SDK." 
        )
        raise new_exc
    except (subprocess.CalledProcessError, IOError) as caught_exc:
        new_exc = BadUserConfigurationException(
            "Failed to obtain access token. Run `gcloud auth login` in your command line "\
            "to authorize gcloud to access the Cloud Platform with Google user credentials "\
            "to authenticate. Run `gcloud auth application-default login` to acquire new "\
            "user credentials to use for Application Default Credentials.")
        raise new_exc


def get_cluster_pool(project_id, region, client):
    #filter format: status.state = ACTIVE AND clusterName = mycluster AND labels.env = staging AND labels.starred = \*
    cluster_pool = set()
    for cluster in client.list_clusters(project_id, region, 'status.state = ACTIVE'):
        #check component gateway is enabled
        if (len(cluster.config.endpoint_config.http_ports.values()) != 0):
            action_list = list()
            for action in cluster.config.initialization_actions:
                if "livy" in action.executable_file:
                    action_list.append(action.executable_file)
                    cluster_pool.add(cluster.cluster_name)
    return cluster_pool


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
                            'api_endpoint': '{}-dataproc.googleapis.com:443'.format(region)
                        }
                    )
    try:
        print(get_cluster_pool(project_id, region, client))
        # if they didn't enter cluster name then we get a cluster from cluster pool 
        cluster_pool = None
        if cluster_name is '':
            #pop random cluster from cluster pool 
            cluster_pool = get_cluster_pool(project_id, region, client)
            cluster_name = cluster_pool.copy().pop()
        response = client.get_cluster(project_id, region, cluster_name)
        """
        regex stuff
        init_action = response.config.initialization_actions
        value = init_action.pop()
        ans2 = re.search('(?<=actions-)[a-z0-9-]+', value.executable_file)
        print(get_cluster_pool(project_id, ans2.group(), 'status.state = ACTIVE', credentials))
        """
        url = ((response.config.endpoint_config).http_ports).popitem()[1]
        parsed_uri = urllib3.util.parse_url(url)
        endpoint_address = '{uri.scheme}://{uri.netloc}/'.format(uri=parsed_uri) + 'gateway/default/livy/v1'
        ipython_display = IpythonDisplay()
        
        if cluster_pool is not None:
            ipython_display.writeln("Used {} cluster from cluster_pool: {}".format(cluster_name, cluster_pool))
        else:
            ipython_display.writeln("Used {} cluster".format(cluster_name))
        return endpoint_address
    except:
        raise

def application_default_credentials_configured(): 
    """Checks if google application-default credentials are configured"""
    callable_request = google.auth.transport.requests.Request()
    try:
        credentials, project = google.auth.default(scopes=['https://www.googleapis.com/auth/cloud-platform','https://www.googleapis.com/auth/userinfo.email'])
    except:
        pass
        return False
    return not(credentials is None)


class GoogleAuth(Authenticator):
    """Custom Authenticator to use Google OAuth with SparkMagic."""

    def __init__(self, parsed_attributes=None):
        self.callable_request = google.auth.transport.requests.Request()
        self.credentialed_accounts, active_user_account = list_credentialed_accounts()
        self.scopes = ['https://www.googleapis.com/auth/cloud-platform', 'https://www.googleapis.com/auth/userinfo.email']
        self.active_credentials = None
        self.default_credentials_configured = application_default_credentials_configured()
        
        account_dict = list_accounts_pairs(self.credentialed_accounts, self.default_credentials_configured)
        if parsed_attributes is not None:
            if parsed_attributes.account in account_dict:
                self.active_credentials = parsed_attributes.account
            else: 
                new_exc = BadUserConfigurationException(
                "{} is not a credentialed account. Run `gcloud auth login` in your command line "\
                "to authorize gcloud to access the Cloud Platform with Google user credentials to authenticate. "\
                "Run `gcloud auth application-default login` acquire new user credentials "\
                "to use for Application Default Credentials. Run `gcloud auth list` to see "\
                "your credentialed accounts.".format(parsed_attributes.account))
                raise new_exc
            if self.active_credentials == 'default-credentials' and self.default_credentials_configured:
                self.credentials, self.project = google.auth.default(scopes=self.scopes)
            #fix account_dict
            elif self.active_credentials in account_dict:
                self.credentials = get_credentials_for_account(self.active_credentials, self.scopes)
            else:
                self.credentials, self.project = None, None
        else:
            if self.default_credentials_configured:
                self.credentials, self.project = google.auth.default(scopes=self.scopes)
                self.active_credentials = 'default-credentials'
            #fix this to be active_credentials is in credentialed accounts 
            elif active_user_account is not None:
                self.credentials = get_credentials_for_account(active_user_account, self.scopes)
                self.active_credentials = active_user_account
            else:
                self.credentials, self.project = None, None
        Authenticator.__init__(self, parsed_attributes)
        self.widgets = self.get_widgets(WIDGET_WIDTH)

    def get_widgets(self, widget_width):
        ipywidget_factory = IpyWidgetFactory()

        self.project_widget = ipywidget_factory.get_text(
            description='Project:',
            width=widget_width
        )

        self.cluster_name_widget = ipywidget_factory.get_text(
            description='Cluster:',
            width=widget_width
        )

        self.region_widget = ipywidget_factory.get_text(
            description='Region:',
            width=widget_width
        )

        self.google_credentials_widget = ipywidget_factory.get_dropdown(
            options=list_accounts_pairs(self.credentialed_accounts, self.default_credentials_configured),
            value=None,
            description=u"Account:"
        )

        if self.active_credentials is not None: 
            self.google_credentials_widget.value = self.active_credentials
        else: 
            self.google_credentials_widget.disabled = True

        widgets = [self.project_widget, self.region_widget, self.cluster_name_widget, self.google_credentials_widget]
        return widgets

    def initialize_credentials_with_auth_account_selection(self, account):
        """Initializes self.credentials with the accound selected from the auth dropdown widget"""
        if (account != self.active_credentials):
            if (account == 'default-credentials'):
                self.credentials, self.project = google.auth.default(scopes=self.scopes)
                #self.credentials.refresh(self.callable_request)
            else:
                self.credentials = get_credentials_for_account(account, self.scopes)
                #self.credentials.refresh(self.callable_request)
        
    def update_with_widget_values(self):
        no_credentials_exception = BadUserConfigurationException(
            "Failed to obtain access token. Run `gcloud auth login` in your command line "\
            "to authorize gcloud to access the Cloud Platform with Google user credentials to "\
            "authenticate. Run `gcloud auth application-default login` acquire new user "\
            "credentials to use for Application Default Credentials.")
        if (self.credentials is not None):
            try: 
                self.url = get_component_gateway_url(self.project_widget.value, self.region_widget.value, \
                    self.cluster_name_widget.value, self.credentials)
            except: 
                raise
        else: 
            raise no_credentials_exception
        self.initialize_credentials_with_auth_account_selection(self.google_credentials_widget.value)

    def __call__(self, request):
        if not self.credentials.valid:
            self.credentials.refresh(self.callable_request)
        request.headers['Authorization'] = 'Bearer {}'.format(self.credentials.token)
        return request

    def __hash__(self):
        return hash((self.active_credentials, self.url, self.__class__.__name__))
