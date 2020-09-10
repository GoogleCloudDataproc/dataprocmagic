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


"""Tests the `%manage_dataproc` and `%spark` magics"""


from mock import patch, MagicMock, PropertyMock
from nose.tools import raises, assert_equals, with_setup
from google.oauth2 import credentials
import googledataprocauthenticator
from googledataprocauthenticator.google import GoogleAuth
from googledataprocauthenticator.magics.dataprocmagics import DataprocMagics
from sparkmagic.livyclientlib.endpoint import Endpoint
from sparkmagic.livyclientlib.livysession import LivySession
from sparkmagic.livyclientlib.exceptions import BadUserConfigurationException
from sparkmagic.utils.utils import parse_argstring_or_throw, initialize_auth
from sparkmagic.utils.constants import SESSION_KIND_SPARK



magic = None
spark_controller = None
shell = None
ipython_display = None


def _setup():
    with patch('googledataprocauthenticator.magics.dataprocmagics.DataprocMagics.self.db', new_callable=PropertyMock,
           return_value=mocked_db):
        global magic, spark_controller, shell, ipython_display
        magic = DataprocMagics(shell=None, widget=MagicMock())
        magic.shell = shell = MagicMock()
        magic.ipython_display = ipython_display = MagicMock()
        magic.spark_controller = spark_controller = MagicMock()

def _teardown():
    pass

stored_endpoints = ("http://url.com", Endpoint("http://url.com", "default-credentials"))
get_session_id_to_name = {1234: 'my_session'}
sessions_mock = {'my_session': LivySession(http_client=MagicMock(), properties={"kind":SESSION_KIND_SPARK, \
    "heartbeatTimeoutInSecond": 60}, ipython_display=ipython_display, session_id=1234)}
sessions_list_mock = [LivySession(http_client=MagicMock(), properties={"kind":SESSION_KIND_SPARK,\
    "heartbeatTimeoutInSecond": 60}, ipython_display=ipython_display, session_id=1234)]
mocked_db = {'autorestore/stored_endpoints': stored_endpoints, 'autorestore/get_session_id_to_name': get_session_id_to_name,}

def make_credentials():
    return credentials.Credentials(
        token=None,
        refresh_token='refresh',
        token_uri='token_uri',
        client_id='client_id',
        client_secret='client_secret',
    )

creds = make_credentials()
mock_credentialed_accounts_valid_accounts = ({'account@google.com'}, 'account@google.com')
AUTH_DESCRIBE_USER = '{"client_id": "client_id", \
     "client_secret": "secret", "refresh_token": "refresh","type": "authorized_user"}'

@with_setup(_setup, _teardown)
def test_session_command_parses():
    print_info_mock = MagicMock()
    magic._print_local_info = print_info_mock
    command = "session"
    magic.spark(command)
    print_info_mock.assert_called_once_with()

@with_setup(_setup, _teardown)
def test_session_endpoint_command_parses():
    print_info_mock = MagicMock()
    magic._print_endpoint_info = print_info_mock
    command = "session -u http://url.com -i 1234"
    spark_controller.get_all_sessions_endpoint_info = MagicMock(return_value=None)
    magic.spark(command)
    print_info_mock.assert_called_once_with(None, 1234)

@with_setup(_setup, _teardown)
def test_add_sessions_command_parses_google_default_credentials():
    with patch('google.auth.default', return_value=(creds, 'project'), \
    autospec=True):
        add_sessions_mock = MagicMock()
        spark_controller.add_session = add_sessions_mock
        command = "add"
        name = "-s name"
        language = "-l python"
        account = "-g default-credentials"
        connection_string = "-u http://url.com -t Google"
        line = " ".join([command, name, language, connection_string, account])
        magic.spark(line)
        args = parse_argstring_or_throw(DataprocMagics.spark, line)
        auth_instance = initialize_auth(args)
        add_sessions_mock.assert_called_once_with("name", Endpoint("http://url.com", initialize_auth(args)),
                                                  False, {"kind": "pyspark"})
        assert_equals(auth_instance.url, "http://url.com")
        isinstance(auth_instance, GoogleAuth)
        assert_equals(auth_instance.active_credentials, 'default-credentials')

@with_setup(_setup, _teardown)
def test_add_sessions_command_parses_google_user_credentials():
    with patch('sparkmagic.auth.google.list_credentialed_user_accounts', \
    return_value=mock_credentialed_accounts_valid_accounts), patch('subprocess.check_output',\
    return_value=AUTH_DESCRIBE_USER):
        add_sessions_mock = MagicMock()
        spark_controller.add_session = add_sessions_mock
        command = "add"
        name = "-s name"
        language = "-l python"
        account = "-g account@google.com"
        connection_string = "-u http://url.com -t Google"
        line = " ".join([command, name, language, connection_string, account])
        magic.spark(line)
        args = parse_argstring_or_throw(DataprocMagics.spark, line)
        auth_instance = initialize_auth(args)
        add_sessions_mock.assert_called_once_with("name", Endpoint("http://url.com", initialize_auth(args)),
                                                  False, {"kind": "pyspark"})
        assert_equals(auth_instance.url, "http://url.com")
        isinstance(auth_instance, GoogleAuth)
        assert_equals(auth_instance.active_credentials, 'account@google.com')

@with_setup(_setup, _teardown)
def test_add_sessions_command_parses_session_already_exists():
    spark_controller.get_all_sessions_endpoint = MagicMock(return_value=sessions_list_mock)
    get_managed_clients_mock = MagicMock(return_value=sessions_mock)
    spark_controller.get_managed_clients = get_managed_clients_mock
    add_sessions_mock = MagicMock()
    spark_controller.session_manager.add_session = add_sessions_mock
    command = "add"
    name = "-s my_session"
    language = "-l python"
    connection_string = "-u http://url.com -t {} -g account@google.com".format('Google')
    line = " ".join([command, name, language, connection_string])
    magic.spark(line)
    assert_equals(magic.db['autorestore/stored_endpoints'], stored_endpoints)
    assert_equals(magic.db['autorestore/get_session_id_to_name'], get_session_id_to_name)
    add_sessions_mock.assert_not_called()

@raises(BadUserConfigurationException)
@with_setup(_setup, _teardown)
def test_add_sessions_command_raises_google_no_account():
    with patch('google.auth.default', return_value=(creds, 'project'), \
    autospec=True):
        add_sessions_mock = MagicMock()
        spark_controller.add_session = add_sessions_mock
        command = "add"
        name = "-s name"
        language = "-l python"
        connection_string = "-u http://url.com -t Google"
        line = " ".join([command, name, language, connection_string])
        magic.spark(line)
        args = parse_argstring_or_throw(DataprocMagics.spark, line)
        initialize_auth(args)

@with_setup(_setup, _teardown)
def test_restore_endpoints():
    with patch('google.auth.default', return_value=(creds, 'project'),\
    autospec=True):
        assert_equals(magic.endpoints, stored_endpoints)

@with_setup(_setup, _teardown)
def test_restore_sessions():
    with patch('google.auth.default', return_value=(creds, 'project'),\
    autospec=True):
        spark_controller.get_all_sessions_endpoint = MagicMock(return_value=sessions_list_mock)
        spark_controller.get_managed_clients = []
        add_sessions_mock = MagicMock()
        spark_controller.session_manager.add_session = add_sessions_mock
        add_sessions_mock.assert_called_once_with("my_session", LivySession(http_client=MagicMock(),\
    properties={"kind":SESSION_KIND_SPARK, "heartbeatTimeoutInSecond": 60}, ipython_display=ipython_display, session_id=12345))
        assert_equals(spark_controller, stored_endpoints)
