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

"""Runs Scala, PySpark and SQL statement through Spark using a REST endpoint in remote cluster.
Provides the %spark and %manage_dataproc magics."""

from IPython.core.magic import magics_class, line_cell_magic, needs_local_scope, line_magic
from IPython.core.magic_arguments import argument, magic_arguments
from hdijupyterutils.ipywidgetfactory import IpyWidgetFactory
from sparkmagic.utils.utils import parse_argstring_or_throw, initialize_auth, Namespace
from sparkmagic.livyclientlib.endpoint import Endpoint
from sparkmagic.livyclientlib.exceptions import handle_expected_exceptions
from sparkmagic.magics.remotesparkmagics import RemoteSparkMagics
from sparkmagic.magics.sparkmagicsbase import SparkMagicBase
from sparkmagic.controllerwidget.magicscontrollerwidget import MagicsControllerWidget
import sparkmagic.utils.configuration as conf
from sparkmagic.utils.constants import LANG_PYTHON, CONTEXT_NAME_SPARK, CONTEXT_NAME_SQL, LANG_SCALA, LANG_R
from googledataprocauthenticator.controllerwidget.controllerwidget import ControllerWidget


@magics_class
class DataprocMagics(SparkMagicBase):

    def __init__(self, shell, data=None, widget=None):
        super(DataprocMagics, self).__init__(shell, data)
        self.ip = self.shell
        self.db = self.ip.db
        self.endpoints = {}
        stored_endpoints = self.get_stored_endpoints()
        for endpoint_tuple in stored_endpoints:
            self._load_sessions_for_endpoint(endpoint_tuple)
        # update session_id_to_name to be all of the sessions now loaded into session_manager
        session_id_to_name = dict([(session.id, name) for name, session in self.spark_controller.get_managed_clients().items()])
        self.db['autorestore/' + 'session_id_to_name'] = session_id_to_name
        if len(stored_endpoints) == 0:
            self.endpoints = None
        dataproc_widget = ControllerWidget(self.spark_controller, IpyWidgetFactory(), self.ipython_display, self.db, self.endpoints)
        widget = MagicsControllerWidget(self.spark_controller, IpyWidgetFactory(), self.ipython_display, self.endpoints)
        if self.endpoints is None:
            self.endpoints = {}
        self.manage_dataproc_widget = dataproc_widget
        self.__remotesparkmagics = RemoteSparkMagics(shell, widget)

    def get_stored_endpoints(self):
        """Gets a list of endpoints that were added in previous notebook sessions

        Returns:
            stored_endpoints (Sequence[tuple]): A list of tuples with two str values
            (url, account) where url is an endpoint url and account is the credentialed
            account used to authenticate the endpoint connection. If no endpoints can be
            obtained from previous notebook sessions, an empty list is returned.
        """
        try:
            stored_endpoints = self.db['autorestore/' + 'stored_endpoints']
            return stored_endpoints
        except Exception as caught_exc:
            self.db['autorestore/' + 'stored_endpoints'] = list()
            self.ipython_display.send_error("Failed to restore stored_endpoints from a previous "\
            f"notebook session due to an error: {str(caught_exc)}. Cleared stored_endpoints.")
            return list()

    def get_session_id_to_name(self):
        """Gets a dictionary that maps currently running livy session id's to their names

        Returns:
            session_id_to_name (dict): a dictionary mapping session.id -> name
            If no sessions can be obtained from previous notebook sessions, an
            empty dict is returned.
        """
        try:
            session_id_to_name = self.db['autorestore/' + 'session_id_to_name']
            return session_id_to_name
        except Exception as caught_exc:
            self.db['autorestore/' + 'session_id_to_name'] = dict()
            self.ipython_display.send_error("Failed to restore session_id_to_name from a previous "\
            f"notebook session due to an error: {str(caught_exc)}. Cleared session_id_to_name.")
            return dict()

    def _load_sessions_for_endpoint(self, endpoint_tuple):
        """Loads all of the running livy sessions of an endpoint

        Args:
            endpoint_tuple (tuple): a tuple of two strings in the format (url, account) where url is
            the endpoint url and account is the credentialed account used to authenticate
        """
        args = Namespace(auth='Google', url=endpoint_tuple[0], account=endpoint_tuple[1])
        auth = initialize_auth(args)
        endpoint = Endpoint(url=endpoint_tuple[0], auth=auth)
        self.endpoints[endpoint.url] = endpoint
        session_id_to_name = self.get_session_id_to_name()
        #get all sessions running on that endpoint
        endpoint_sessions = self.spark_controller.get_all_sessions_endpoint(endpoint)
        #add each session to session manager.
        for session in endpoint_sessions:
            name = session_id_to_name.get(session.id)
            if name is not None and name not in self.spark_controller.get_managed_clients():
                self.spark_controller.session_manager.add_session(name, session)

    @line_magic
    def manage_dataproc(self, line, local_ns=None):
        """Magic to manage Spark endpoints and sessions for Dataproc. First, add an endpoint via the 'Add Endpoint' tab.
        Then, create a session."""
        print(f"dataproc magic {self.endpoints}")
        self.manage_dataproc_widget = ControllerWidget(self.spark_controller, IpyWidgetFactory(), self.ipython_display, self.db, self.endpoints)
        return self.manage_dataproc_widget
    
    @line_magic
    def manage_spark(self, line, local_ns=None):
        """Magic to manage Spark endpoints and sessions. First, add an endpoint via the 'Add Endpoint' tab.
        Then, create a session. You'll be able to select the session created from the %%spark magic."""
        return self.__remotesparkmagics.manage_widget
    
    @magic_arguments()
    @argument("-c", "--context", type=str, default=CONTEXT_NAME_SPARK,
              help="Context to use: '{}' for spark and '{}' for sql queries. "
                   "Default is '{}'.".format(CONTEXT_NAME_SPARK, CONTEXT_NAME_SQL, CONTEXT_NAME_SPARK))
    @argument("-s", "--session", type=str, default=None, help="The name of the Livy session to use.")
    @argument("-o", "--output", type=str, default=None, help="If present, output when using SQL "
                                                             "queries will be stored in this variable.")
    @argument("-q", "--quiet", type=bool, default=False, nargs="?", const=True, help="Do not display visualizations"
                                                                                     " on SQL queries")
    @argument("-m", "--samplemethod", type=str, default=None, help="Sample method for SQL queries: either take or sample")
    @argument("-n", "--maxrows", type=int, default=None, help="Maximum number of rows that will be pulled back "
                                                                        "from the server for SQL queries")
    @argument("-r", "--samplefraction", type=float, default=None, help="Sample fraction for sampling from SQL queries")
    @argument("-u", "--url", type=str, default=None, help="URL for Livy endpoint")
    @argument("-a", "--user", dest='user', type=str, default="", help="Username for HTTP access to Livy endpoint")
    @argument("-p", "--password", type=str, default="", help="Password for HTTP access to Livy endpoint")
    @argument("-t", "--auth", type=str, default=None, help="Auth type for HTTP access to Livy endpoint. [Kerberos, None, Basic]")
    @argument("-l", "--language", type=str, default=None,
              help="Language for Livy session; one of {}".format(', '.join([LANG_PYTHON, LANG_SCALA, LANG_R])))
    @argument("command", type=str, default=[""], nargs="*", help="Commands to execute.")
    @argument("-k", "--skip", type=bool, default=False, nargs="?", const=True, help="Skip adding session if it already exists")
    @argument("-i", "--id", type=int, default=None, help="Session ID")
    @argument("-e", "--coerce", type=str, default=None, help="Whether to automatically coerce the types (default, pass True if being explicit) "
                                                                        "of the dataframe or not (pass False)")
    @argument("-g", "--credentials", dest='account', type=str, default=None, help="Credentials for Google authentication. [account@google.com, "
                                                                        "default-credentials]")

    @needs_local_scope
    @line_cell_magic
    @handle_expected_exceptions
    def spark(self, line, cell="", local_ns=None):
        """Magic to execute spark remotely.

           This magic allows you to create a Livy Scala or Python session against a Livy endpoint. Every session can
           be used to execute either Spark code or SparkSQL code by executing against the SQL context in the session.
           When the SQL context is used, the result will be a Pandas dataframe of a sample of the results.

           If invoked with no subcommand, the cell will be executed against the specified session.

           Subcommands
           -----------
           info
               Display the available Livy sessions and other configurations for sessions with None, Basic, or Kerberos auth.
           sessions
               Display the available Livy sessions and other configurations for sessions created with Google Authentication.
           add
               Add a Livy session given a session name (-s), language (-l), and endpoint credentials.
               The -k argument, if present, will skip adding this session if it already exists.
               e.g. `%spark add -s test -l python -u https://sparkcluster.net/livy -t Kerberos -a u -p -k`
           config
               Override the livy session properties sent to Livy on session creation. All session creations will
               contain these config settings from then on.
               Expected value is a JSON key-value string to be sent as part of the Request Body for the POST /sessions
               endpoint in Livy.
               e.g. `%%spark config`
                    `{"driverMemory":"1000M", "executorCores":4}`
           run
               Run Spark code against a session.
               e.g. `%%spark -s testsession` will execute the cell code against the testsession previously created
               e.g. `%%spark -s testsession -c sql` will execute the SQL code against the testsession previously created
               e.g. `%%spark -s testsession -c sql -o my_var` will execute the SQL code against the testsession
                        previously created and store the pandas dataframe created in the my_var variable in the
                        Python environment.
           logs
               Returns the logs for a given session.
               e.g. `%spark logs -s testsession` will return the logs for the testsession previously created
           delete
               Delete a Livy session.
               e.g. `%spark delete -s defaultlivy`
           cleanup
               Delete all Livy sessions created by the notebook. No arguments required.
               e.g. `%spark cleanup`
        """
        usage = "Please look at usage of %spark by executing `%spark?`."
        user_input = line
        args = parse_argstring_or_throw(self.spark, user_input)
        subcommand = args.command[0].lower()
        if args.auth == "Google" and subcommand == "add":
            if args.url is None:
                self.ipython_display.send_error("Need to supply URL argument (e.g. -u https://example.com/livyendpoint)")
                return
            name = args.session
            language = args.language
            endpoint = Endpoint(args.url, initialize_auth(args))
            self.endpoints[args.url] = endpoint
            # convert self.endpoints dict into list of (url, account) tuples
            stored_endpoints = [(url, endpoint.auth.active_credentials) for url, endpoint in self.endpoints.items()]
            # stored updated stored_endpoints
            self.db['autorestore/' + 'stored_endpoints'] = stored_endpoints
            skip = args.skip
            properties = conf.get_session_properties(language)
            self.spark_controller.add_session(name, endpoint, skip, properties)
            # session_id_to_name dict is necessary to restore session name across notebook sessions
            # since the livy server does not store the name.
            session_id_to_name = self.get_session_id_to_name()
            # add session id -> name to session_id_to_name dict
            session_id_to_name[self.spark_controller.session_manager.get_session(name).id] = name
            self.db[ 'autorestore/' + 'session_id_to_name'] = session_id_to_name
        elif subcommand == "sessions":
            if args.url is not None and args.id is not None:
                endpoint = Endpoint(args.url, initialize_auth(args))
                info_sessions = self.spark_controller.get_all_sessions_endpoint_info(endpoint)
                self._print_endpoint_info(info_sessions, args.id)
            else:
                self._print_local_info()
        else:
            self.__remotesparkmagics.spark(line, cell="", local_ns=None)

    def _print_local_info(self):
        sessions_info = ["        {}".format(i) for i in self.spark_controller.get_manager_sessions_str()]
        print("""Info for running Spark:
    Sessions:
{}
    Session configs:
        {}
""".format("\n".join(sessions_info), conf.session_configs()))

def load_ipython_extension(ip):
    ip.register_magics(RemoteSparkMagics)
    ip.register_magics(DataprocMagics)
