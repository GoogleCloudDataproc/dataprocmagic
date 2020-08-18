# Copyright (c) 2015  aggftw@gmail.com
# Distributed under the terms of the Modified BSD License.
import json
import datetime
from mock import patch, PropertyMock, MagicMock, sentinel, Mock
from nose.tools import raises, assert_equals, with_setup, assert_is_not_none, assert_false, assert_true, assert_raises
import requests
from requests_kerberos.kerberos_ import HTTPKerberosAuth, REQUIRED, OPTIONAL
from sparkmagic.auth.basic import Basic
from sparkmagic.auth.kerberos import Kerberos
import sparkmagic.auth.google as google_auth_class
import google.auth
from google.auth.exceptions import DefaultCredentialsError
from google.api_core.exceptions import GoogleAPICallError, RetryError
import sparkmagic
from sparkmagic.auth.google import GoogleAuth
from sparkmagic.auth.customauth import Authenticator
from sparkmagic.livyclientlib.endpoint import Endpoint
from sparkmagic.livyclientlib.exceptions import HttpClientException
from sparkmagic.livyclientlib.exceptions import BadUserConfigurationException
from sparkmagic.livyclientlib.linearretrypolicy import LinearRetryPolicy
from sparkmagic.livyclientlib.reliablehttpclient import ReliableHttpClient
import sparkmagic.utils.configuration as conf
import sparkmagic.utils.constants as constants
from unittest.mock import create_autospec, call
from google.oauth2 import credentials
from google.cloud import dataproc_v1beta2
import os
import subprocess
import google

retry_policy = None
sequential_values = []
google_auth_instance = GoogleAuth()
endpoint = Endpoint("http://url.com", google_auth_instance)

def _setup():
    global retry_policy
    retry_policy = LinearRetryPolicy(0.01, 5)

def _teardown():
    pass

def return_sequential():
    global sequential_values
    val = sequential_values[0]
    sequential_values = sequential_values[1:]
    return val


@with_setup(_setup, _teardown)
def test_get():
    with patch('requests.Session.get') as patched_get:
        type(patched_get.return_value).status_code = 200
        client = ReliableHttpClient(endpoint, {}, retry_policy)

        result = client.get("r", [200])

        assert_equals(200, result.status_code)


@raises(HttpClientException)
@with_setup(_setup, _teardown)
def test_get_throws():
    with patch('requests.Session.get') as patched_get:
        type(patched_get.return_value).status_code = 500

        client = ReliableHttpClient(endpoint, {}, retry_policy)

        client.get("r", [200])


@with_setup(_setup, _teardown)
def test_get_will_retry():
    global sequential_values, retry_policy
    retry_policy = MagicMock()
    retry_policy.should_retry.return_value = True
    retry_policy.seconds_to_sleep.return_value = 0.01

    with patch('requests.Session.get') as patched_get:
        # When we call assert_equals in this unit test, the side_effect is executed.
        # So, the last status_code should be repeated.
        sequential_values = [500, 200, 200]
        pm = PropertyMock()
        pm.side_effect = return_sequential
        type(patched_get.return_value).status_code = pm
        client = ReliableHttpClient(endpoint, {}, retry_policy)

        result = client.get("r", [200])

        assert_equals(200, result.status_code)
        retry_policy.should_retry.assert_called_once_with(500, False, 0)
        retry_policy.seconds_to_sleep.assert_called_once_with(0)


@with_setup(_setup, _teardown)
def test_post():
    with patch('requests.Session.post') as patched_post:
        type(patched_post.return_value).status_code = 200

        client = ReliableHttpClient(endpoint, {}, retry_policy)

        result = client.post("r", [200], {})

        assert_equals(200, result.status_code)


@raises(HttpClientException)
@with_setup(_setup, _teardown)
def test_post_throws():
    with patch('requests.Session.post') as patched_post:
        type(patched_post.return_value).status_code = 500

        client = ReliableHttpClient(endpoint, {}, retry_policy)

        client.post("r", [200], {})


@with_setup(_setup, _teardown)
def test_post_will_retry():
    global sequential_values, retry_policy
    retry_policy = MagicMock()
    retry_policy.should_retry.return_value = True
    retry_policy.seconds_to_sleep.return_value = 0.01

    with patch('requests.Session.post') as patched_post:
        # When we call assert_equals in this unit test, the side_effect is executed.
        # So, the last status_code should be repeated.
        sequential_values = [500, 200, 200]
        pm = PropertyMock()
        pm.side_effect = return_sequential
        type(patched_post.return_value).status_code = pm
        client = ReliableHttpClient(endpoint, {}, retry_policy)

        result = client.post("r", [200], {})

        assert_equals(200, result.status_code)
        retry_policy.should_retry.assert_called_once_with(500, False, 0)
        retry_policy.seconds_to_sleep.assert_called_once_with(0)


@with_setup(_setup, _teardown)
def test_delete():
    with patch('requests.Session.delete') as patched_delete:
        type(patched_delete.return_value).status_code = 200

        client = ReliableHttpClient(endpoint, {}, retry_policy)

        result = client.delete("r", [200])

        assert_equals(200, result.status_code)

@raises(HttpClientException)
@with_setup(_setup, _teardown)
def test_delete_throws():
    with patch('requests.Session.delete') as patched_delete:
        type(patched_delete.return_value).status_code = 500

        client = ReliableHttpClient(endpoint, {}, retry_policy)

        client.delete("r", [200])


@with_setup(_setup, _teardown)
def test_delete_will_retry():
    global sequential_values, retry_policy
    retry_policy = MagicMock()
    retry_policy.should_retry.return_value = True
    retry_policy.seconds_to_sleep.return_value = 0.01

    with patch('requests.Session.delete') as patched_delete:
        # When we call assert_equals in this unit test, the side_effect is executed.
        # So, the last status_code should be repeated.
        sequential_values = [500, 200, 200]
        pm = PropertyMock()
        pm.side_effect = return_sequential
        type(patched_delete.return_value).status_code = pm
        client = ReliableHttpClient(endpoint, {}, retry_policy)

        result = client.delete("r", [200])

        assert_equals(200, result.status_code)
        retry_policy.should_retry.assert_called_once_with(500, False, 0)
        retry_policy.seconds_to_sleep.assert_called_once_with(0)


@with_setup(_setup, _teardown)
def test_will_retry_error_no():
    global sequential_values, retry_policy
    retry_policy = MagicMock()
    retry_policy.should_retry.return_value = False
    retry_policy.seconds_to_sleep.return_value = 0.01

    with patch('requests.Session.get') as patched_get:
        patched_get.side_effect = requests.exceptions.ConnectionError()
        client = ReliableHttpClient(endpoint, {}, retry_policy)

        try:
            client.get("r", [200])
            assert False
        except HttpClientException:
            retry_policy.should_retry.assert_called_once_with(None, True, 0)


@with_setup(_setup, _teardown)
def test_google_auth():
    endpoint = Endpoint("http://url.com", google_auth_instance)
    client = ReliableHttpClient(endpoint, {}, retry_policy)
    assert_is_not_none(client._auth)
    assert isinstance(client._auth, GoogleAuth)
    assert hasattr(client._auth, 'url')
    assert hasattr(client._auth, 'widgets')
    
MOCK_GOOGLE = Mock(spec=GoogleAuth)
MOCK_CREDENTIALS = Mock(spec = credentials.Credentials)
def make_credentials():
    return credentials.Credentials(
        token=None,
        refresh_token='refresh',
        token_uri='token_uri',
        client_id='client_id',
        client_secret='client_secret',
    )

creds = make_credentials()
def test_default_credentials_configured_credentials_is_not_none():
    with patch('google.auth.default', return_value=(creds, 'project'), \
    autospec=True):
        assert_equals(GoogleAuth().credentials, creds)
        assert_is_not_none(GoogleAuth().credentials)

def test_default_credentials_not_configured_credentials_is_none():
    with patch('google.auth.default', side_effect=DefaultCredentialsError, \
    autospec=True):
        assert_equals(GoogleAuth().credentials, None)

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

#MOCK_CREDENTIALS = Mock(spec = credentials.Credentials)
auth_list = '[{"account": "account@google.com","status": "ACTIVE"}]'
creds_set = set()
creds_set.add('account@google.com')
mock_credentialed_accounts = (creds_set, 'account@google.com')


def test_active_account_returns_valid_active_account():
    with patch('subprocess.check_output', return_value=auth_list), \
    patch('google.auth.default', side_effect=DefaultCredentialsError), \
    patch('google.auth._cloud_sdk.get_auth_access_token', return_value='token'):
        _, active_account = sparkmagic.auth.google.list_credentialed_accounts()
        print(active_account)
        assert_equals(active_account, 'account@google.com')

def test_dropdown_options_with_default_credentials_configured():
    with patch('subprocess.check_output', return_value=auth_list), \
        patch('google.auth.default', return_value=(MOCK_CREDENTIALS, 'project')), \
        patch('google.auth._cloud_sdk.get_auth_access_token', return_value='token'):
        assert_equals(GoogleAuth().google_credentials_widget.options, {'account@google.com':'account@google.com', \
            'default-credentials':'default-credentials'})


def refreshed_credentials():
    return credentials.Credentials(
            token='token',
            refresh_token='refresh',
            token_uri='token_uri',
            client_id='client_id',
            client_secret='client_secret',
        )

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
    with patch('subprocess.check_output', return_value=auth_list), \
    patch('google.auth.default', return_value=(not_refreshed_credentials(), 'project')) as d, \
    patch('google.auth._cloud_sdk.get_auth_access_token', return_value='token'), \
    patch('google.oauth2._client.refresh_grant', return_value=('token', 'refresh', \
    expiry, grant_response)), \
    patch('sparkmagic.auth.google.list_credentialed_accounts', return_value=mock_credentialed_accounts):
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
    patch('sparkmagic.auth.google.list_credentialed_accounts', return_value=mock_credentialed_accounts):
        google_auth = GoogleAuth()
        print(google_auth_class.list_credentialed_accounts)
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
    with patch('subprocess.check_output', return_value=auth_list), \
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
    patch('sparkmagic.auth.google.list_credentialed_accounts', return_value=auth_list), \
    patch('google.auth._cloud_sdk.get_auth_access_token', return_value='token'), \
    patch('google.oauth2._client.refresh_grant', return_value=('token', 'refresh', \
    expiry, grant_response)), \
    patch('sparkmagic.auth.google.list_credentialed_accounts', return_value=mock_credentialed_accounts):
        google_auth = GoogleAuth()
        assert_equals(google_auth.active_credentials, 'account@google.com')
        assert_equals(google_auth.credentials.client_secret, 'secret')
        assert_equals(google_auth.credentials.token, None)

#add default credentials dropdown change
def test_call_default_credentials_no_dropdown_change(): 
    with patch('subprocess.check_output', return_value=auth_list), \
    patch('google.auth.default', return_value=(not_refreshed_credentials(), 'project')), \
    patch('google.auth._cloud_sdk.get_auth_access_token', return_value='token'), \
    patch('google.oauth2._client.refresh_grant', return_value=('token', 'refresh', \
    expiry, grant_response)):
        google_auth = GoogleAuth()
        google_auth.initialize_credentials_with_auth_account_selection('default-credentials')
        request = requests.Request(url="http://www.example.org")
        google_auth.__call__(request)
        assert_true('Authorization' in request.headers)
        assert_equals(request.headers['Authorization'],'Bearer {}'.format(google_auth.credentials.token))

#add user credentials dropdown change
def test_call_user_credentials_no_dropdown_change(): 
    with patch('subprocess.check_output', return_value=AUTH_DESCRIBE_USER), \
    patch('google.auth.default', return_value=(DefaultCredentialsError, 'project')), \
    patch('google.auth._cloud_sdk.get_auth_access_token', return_value='token'), \
    patch('google.oauth2._client.refresh_grant', return_value=('token', 'refresh', \
    expiry, grant_response)), \
    patch('sparkmagic.auth.google.list_credentialed_accounts', return_value=mock_credentialed_accounts):
        google_auth = GoogleAuth()
        google_auth.initialize_credentials_with_auth_account_selection('account@google.com')
        request = requests.Request(url="http://www.example.org")
        google_auth.__call__(request)
        assert_true('Authorization' in request.headers)
        assert_equals(request.headers['Authorization'],'Bearer {}'.format(google_auth.credentials.token))
