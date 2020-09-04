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


"""Tests the Google Cloud Dataproc Authenticator for Sparkmagic"""


import datetime
from unittest.mock import call
from mock import patch, Mock
from nose.tools import raises, assert_equals, assert_is_not_none, assert_false, assert_true, assert_raises
import requests
from google.oauth2 import credentials
from google.cloud import dataproc_v1beta2
import google
import google.auth
from google.auth.exceptions import DefaultCredentialsError
from google.api_core.exceptions import GoogleAPICallError, RetryError
import sparkmagic
import sparkmagic.auth.google as google_auth_class
from sparkmagic.auth.google import GoogleAuth
from sparkmagic.livyclientlib.endpoint import Endpoint
from sparkmagic.livyclientlib.exceptions import BadUserConfigurationException
from sparkmagic.livyclientlib.linearretrypolicy import LinearRetryPolicy
from sparkmagic.livyclientlib.reliablehttpclient import ReliableHttpClient


def test_get_google():
    retry_policy = LinearRetryPolicy(0.01, 5)
    with patch('requests.Session.get') as patched_get:
        type(patched_get.return_value).status_code = 200
        endpoint = Endpoint("http://url.com", GoogleAuth())
        client = ReliableHttpClient(endpoint, {}, retry_policy)
        result = client.get("r", [200])
        assert_equals(200, result.status_code)

def test_google_auth():
    retry_policy = LinearRetryPolicy(0.01, 5)
    endpoint = Endpoint("http://url.com", GoogleAuth())
    client = ReliableHttpClient(endpoint, {}, retry_policy)
    assert_is_not_none(client._auth)
    assert isinstance(client._auth, GoogleAuth)
    assert hasattr(client._auth, 'url')
    assert hasattr(client._auth, 'widgets')

MOCK_GOOGLE = Mock(spec=GoogleAuth)
MOCK_CREDENTIALS = Mock(spec=credentials.Credentials)

def make_credentials():
    return credentials.Credentials(
        token=None,
        refresh_token='refresh',
        token_uri='token_uri',
        client_id='client_id',
        client_secret='client_secret',
    )

creds = make_credentials()
AUTH_LIST = '[{"account": "account@google.com","status": "ACTIVE"}]'
mock_credentialed_accounts_no_accounts = (set(), None)
mock_credentialed_accounts_valid_accounts = ({'account@google.com'}, 'account@google.com')

def test_default_credentials_configured_credentials_is_not_none():
    """Tests GoogleAuth.credentials gets initialized when default credentials are configured"""
    with patch('google.auth.default', return_value=(creds, 'project'), \
    autospec=True):
        assert_equals(GoogleAuth().credentials, creds)
        assert_is_not_none(GoogleAuth().credentials)

def test_default_credentials_not_configured_and_no_active_account_credentials_is_none():
    """Tests GoogleAuth.credentials gets initialized to None when default credentials are
    not configured and the user has no credentialed accounts"""
    with patch('google.auth.default', side_effect=DefaultCredentialsError, \
    autospec=True), patch('sparkmagic.auth.google.list_credentialed_accounts',\
    return_value=mock_credentialed_accounts_no_accounts):
        assert_equals(GoogleAuth().credentials, None)

def test_default_credentials_not_configured_credentials_and_active_account_is_not_none():
    """Tests GoogleAuth.credentials gets initialized with active credentialed user account
    when one is available"""
    with patch('google.auth.default', side_effect=DefaultCredentialsError, \
    autospec=True), patch('sparkmagic.auth.google.list_credentialed_accounts', \
    return_value=mock_credentialed_accounts_valid_accounts), patch('subprocess.check_output', \
    return_value=AUTH_DESCRIBE_USER):
        assert_is_not_none(GoogleAuth().credentials)

def test_default_credentials_not_configured_account_pairs_contains_no_default():
    """Tests default-credentials is not in google credentials dropdown if if default credentials
    are not configured"""
    with patch('google.auth.default', side_effect=DefaultCredentialsError, \
    autospec=True):
        assert_false('default-credentials' in GoogleAuth().google_credentials_widget.options)

def test_default_credentials_configured_account_pairs_contains_default():
    """Tests default-credentials is in google credentials dropdown if if default credentials
    are configured""" 
    with patch('google.auth.default', return_value=(MOCK_CREDENTIALS, 'project'), \
    autospec=True):
        assert_true('default-credentials' in GoogleAuth().google_credentials_widget.options)

def test_active_account_returns_valid_active_account():
    with patch('subprocess.check_output', return_value=AUTH_LIST), \
    patch('google.auth.default', side_effect=DefaultCredentialsError), \
    patch('google.auth._cloud_sdk.get_auth_access_token', return_value='token'):
        _, active_account = sparkmagic.auth.google.list_credentialed_accounts()
        assert_equals(active_account, 'account@google.com')

def test_dropdown_options_with_default_credentials_configured():
    with patch('subprocess.check_output', return_value=AUTH_LIST), \
        patch('google.auth.default', return_value=(MOCK_CREDENTIALS, 'project')), \
        patch('google.auth._cloud_sdk.get_auth_access_token', return_value='token'):
        assert_equals(GoogleAuth().google_credentials_widget.options, {'account@google.com':'account@google.com', \
            'default-credentials':'default-credentials'})

def not_refreshed_credentials():
    return credentials.Credentials(
            token=None,
            refresh_token='refresh',
            token_uri='token_uri',
            client_id='client_id',
            client_secret='client_secret',
        )

grant_response = {"id_token": 'id_token'}
expiry = datetime.datetime(2007, 12, 6, 16, 29, 43, 79043)

def test_initialize_credentials_with_auth_dropdown_default_credentials_to_default_credentials():
    """If Google Authenticator is initialized with default-credentials, if the account dropdown is not
    changed to a different account, credentials should not be reinitialized when the endpoint is added."""
    with patch('subprocess.check_output', return_value=AUTH_LIST), \
    patch('google.auth.default', return_value=(not_refreshed_credentials(), 'project')) as d, \
    patch('google.auth._cloud_sdk.get_auth_access_token', return_value='token'), \
    patch('google.oauth2._client.refresh_grant', return_value=('token', 'refresh', \
    expiry, grant_response)), \
    patch('sparkmagic.auth.google.list_credentialed_accounts', return_value=mock_credentialed_accounts_valid_accounts):
        google_auth = GoogleAuth()
        assert_equals(google_auth.active_credentials, 'default-credentials')
        google_auth.initialize_credentials_with_auth_account_selection(google_auth.active_credentials)
        assert_equals(google_auth.active_credentials, 'default-credentials')
        d.assert_has_calls([call(scopes=google_auth.scopes), call(scopes=google_auth.scopes)])

def test_initialize_credentials_with_auth_dropdown_user_credentials_to_user_credentials():
    """If Google Authenticator is initialized with user credentials, if the account dropdown is not
    changed to a different account, credentials should not be reinitialized when the endpoint is added."""
    with patch('subprocess.check_output', return_value=AUTH_DESCRIBE_USER), \
    patch('google.auth.default', side_effect=DefaultCredentialsError), \
    patch('google.auth._cloud_sdk.get_auth_access_token', return_value='token'), \
    patch('google.oauth2._client.refresh_grant', return_value=('token', 'refresh', \
    expiry, grant_response)), \
    patch('sparkmagic.auth.google.list_credentialed_accounts', return_value=mock_credentialed_accounts_valid_accounts):
        google_auth = GoogleAuth()
        assert_equals(google_auth.active_credentials, 'account@google.com')
        google_auth.initialize_credentials_with_auth_account_selection(google_auth.active_credentials)
        google.auth.default.assert_called_once_with(scopes=google_auth.scopes)

@raises(RetryError)
def test_generate_component_gateway_url_raises_retry_error():
    with patch('google.cloud.dataproc_v1beta2.ClusterControllerClient.get_cluster', \
        side_effect=RetryError('error message', 'cause')):
        google_auth_class.get_component_gateway_url('project', 'region', 'cluster', make_credentials())

@raises(GoogleAPICallError)
def test_generate_component_gateway_url_raises_google_api_error():
    with patch('google.cloud.dataproc_v1beta2.ClusterControllerClient.get_cluster', \
        side_effect=GoogleAPICallError('error message')):
        google_auth_class.get_component_gateway_url('project', 'region', 'cluster', make_credentials())

def make_credentials_my():
    return credentials.Credentials(
        token='ya29.a0AfH6SMA_XZN9uqR6ShnPdAHXgoNxXzblmw7rqfZYS_2cxB6Q5ylcF6t1atfUkrmZbJ2dcHehwDJShan83WkNLIkNOIhhuMi1zUSDgXnWfWqSHrqJoSoKv7r5KZLesNyPl5QSmeLx7uXuTXx66QaFhLkH4UOWga9ZdQfOSMp35xYx',
        refresh_token='1//06DpNb1ABrvveCgYIARAAGAYSNwF-L9IrNeIMPi0fmyBNP_O_C3epXaaiqLKWmZ5pK9rs8oYHM__-8A4yaLQc_igCvH268v6q6C8',
        token_uri='https://www.googleapis.com/oauth2/v4/token',
        client_id='32555940559.apps.googleusercontent.com',
        client_secret='ZmssLNjJy2998hD4CTg2ejr2',
    )

def make_cluster():
    endpoint_config = dataproc_v1beta2.types.EndpointConfig(http_ports={"HDFS NameNode": \
        "https://redacted-dot-us-central1.dataproc.googleusercontent.com/hdfs/dfshealth.html"})
    cluster_config = dataproc_v1beta2.types.ClusterConfig(endpoint_config=endpoint_config)
    cluster = dataproc_v1beta2.types.Cluster(project_id="project", cluster_name="cluster", config=cluster_config)
    return cluster

def test_generate_component_gateway_url_successful_get_cluster_request():
    with patch('google.cloud.dataproc_v1beta2.ClusterControllerClient.get_cluster', return_value=make_cluster()):
        url = google_auth_class.get_component_gateway_url("project", "region", "cluster", make_credentials())
        assert_equals(url, "https://redacted-dot-us-central1.dataproc.googleusercontent.com/gateway/default/livy/v1")

@raises(BadUserConfigurationException)
def test_no_credenntials_raises_bad_user_configuration_error():
    no_credentials_exception = BadUserConfigurationException(
            "Failed to obtain access token. Run `gcloud auth login` in your command line \
            to authorize gcloud to access the Cloud Platform with Google user credentials to authenticate. Run `gcloud auth \
            application-default login` acquire new user credentials to use for Application Default Credentials."
    )
    google_auth = GoogleAuth()
    google_auth.project_widget.value = 'project_id'
    google_auth.region_widget.value = 'region'
    google_auth.cluster_name_widget.value = 'cluster_name'
    google_auth.credentials = None
    assert_raises(google_auth.update_with_widget_values(), no_credentials_exception)

AUTH_DESCRIBE_USER = '{"client_id": "client_id", \
     "client_secret": "secret", "refresh_token": "refresh","type": "authorized_user"}'
def test_initialize_credentials_with_default_credentials_configured():
    with patch('subprocess.check_output', return_value=AUTH_LIST), \
    patch('google.auth.default', return_value=(not_refreshed_credentials(), 'project')), \
    patch('google.auth._cloud_sdk.get_auth_access_token', return_value='token'), \
    patch('google.oauth2._client.refresh_grant', return_value=('token', 'refresh', \
    expiry, grant_response)):
        google_auth = GoogleAuth()
        assert_equals(google_auth.active_credentials, 'default-credentials')
        assert_equals(google_auth.credentials.client_secret, 'client_secret')
        assert_equals(google_auth.credentials.token, None)

myset = set()
myset.add('account@google.com')
tupleret = (myset, 'account@google.com')
AUTH_DESCRIBE_USER = '{"client_id": "client_id", \
     "client_secret": "secret", "refresh_token": "refresh","type": "authorized_user"}'
def test_initialize_credentials_with_no_default_credentials_configured():
    with patch('subprocess.check_output', return_value=AUTH_DESCRIBE_USER), \
    patch('google.auth.default', side_effect=DefaultCredentialsError), \
    patch('sparkmagic.auth.google.list_credentialed_accounts', return_value=AUTH_LIST), \
    patch('google.auth._cloud_sdk.get_auth_access_token', return_value='token'), \
    patch('google.oauth2._client.refresh_grant', return_value=('token', 'refresh', \
    expiry, grant_response)), \
    patch('sparkmagic.auth.google.list_credentialed_accounts', return_value=mock_credentialed_accounts_valid_accounts):
        google_auth = GoogleAuth()
        assert_equals(google_auth.active_credentials, 'account@google.com')
        assert_equals(google_auth.credentials.client_secret, 'secret')
        assert_equals(google_auth.credentials.token, None)

def test_call_default_credentials_no_dropdown_change(): 
    with patch('subprocess.check_output', return_value=AUTH_LIST), \
    patch('google.auth.default', return_value=(not_refreshed_credentials(), 'project')), \
    patch('google.auth._cloud_sdk.get_auth_access_token', return_value='token'), \
    patch('google.oauth2._client.refresh_grant', return_value=('token', 'refresh', \
    expiry, grant_response)):
        google_auth = GoogleAuth()
        google_auth.initialize_credentials_with_auth_account_selection('default-credentials')
        request = requests.Request(url="http://www.example.org")
        google_auth.__call__(request)
        assert_true('Authorization' in request.headers)
        assert_equals(request.headers['Authorization'], 'Bearer {}'.format(google_auth.credentials.token))

def test_call_user_credentials_no_dropdown_change(): 
    with patch('subprocess.check_output', return_value=AUTH_DESCRIBE_USER), \
    patch('google.auth.default', return_value=(DefaultCredentialsError, 'project')), \
    patch('google.auth._cloud_sdk.get_auth_access_token', return_value='token'), \
    patch('google.oauth2._client.refresh_grant', return_value=('token', 'refresh', \
    expiry, grant_response)), \
    patch('sparkmagic.auth.google.list_credentialed_accounts', return_value=mock_credentialed_accounts_valid_accounts):
        google_auth = GoogleAuth()
        google_auth.initialize_credentials_with_auth_account_selection('account@google.com')
        request = requests.Request(url="http://www.example.org")
        google_auth.__call__(request)
        assert_true('Authorization' in request.headers)
        assert_equals(request.headers['Authorization'], 'Bearer {}'.format(google_auth.credentials.token))
