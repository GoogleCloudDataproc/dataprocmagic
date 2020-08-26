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
from IPython import get_ipython
from IPython.extensions.storemagic import StoreMagics
from IPython.core.magic import magics_class, line_cell_magic, needs_local_scope, line_magic
from IPython.core.magic_arguments import argument, magic_arguments
from hdijupyterutils.ipywidgetfactory import IpyWidgetFactory
from sparkmagic.utils.utils import parse_argstring_or_throw, get_coerce_value, initialize_auth, Namespace
from sparkmagic.livyclientlib.endpoint import Endpoint
from sparkmagic.livyclientlib.exceptions import handle_expected_exceptions
from sparkmagic.magics.remotesparkmagics import RemoteSparkMagics
from sparkmagic.magics.sparkmagicsbase import SparkMagicBase
from sparkmagic.controllerwidget.magicscontrollerwidget import MagicsControllerWidget
import sparkmagic.utils.configuration as conf
from sparkmagic.utils.constants import LANG_PYTHON, CONTEXT_NAME_SPARK, CONTEXT_NAME_SQL, LANG_SCALA, LANG_R

ipython = get_ipython()

@magics_class
class DataprocMagics(SparkMagicBase):

    def __init__(self, shell, data=None, widget=None):
        # You must call the parent constructor
        super(DataprocMagics, self).__init__(shell, data)
        # load endpoints from saved.
        self.ipython = get_ipython()
        #stored_endpoints = list()
        self.endpoints = {}
        #print(stored_endpoints)
        
        self.ipython.run_line_magic('store', '-r stored_endpoints')
        # need try because if stored_endpoints is not stored, then it will error.
        try: 
            for endpoint_tuple in self.ipython.user_ns['stored_endpoints']:
                args = Namespace(auth='Google', url=endpoint_tuple[0], account=endpoint_tuple[1])
                auth = initialize_auth(args)
                endpoint = Endpoint(url=endpoint_tuple[0], auth=auth)
                self.endpoints[endpoint.url] = endpoint
                #get all sessions running on that endpoint
                endpoint_sessions = self.spark_controller.get_all_sessions_endpoint(endpoint)
                #add each session to session manager.
                for session in endpoint_sessions:
                    print(session)
                    name = self.spark_controller.session_manager.get_session_name_by_id_endpoint(session.id, endpoint)
                    print(name)
                    self.spark_controller.session_manager.add_session(name, session)

        except Exception as e:
            #if it errors then that means we have never stored_endpoints before. 
            #we set it to the 
            self.ipython.user_ns['stored_endpoints'] = list()
            self.ipython.run_line_magic('store', 'stored_endpoints')
            
            self.endpoints = None
            print(e)
       

        """
        print(stored_endpoints)
        
        #self.sessions = []
        for endpoint_tuple in stored_endpoints:
            args = Namespace(auth='Google', url=endpoint_tuple[0], account=endpoint_tuple[1])
            auth = initialize_auth(args)
            endpoint = Endpoint(url=endpoint_tuple[0], auth=auth)
            self.endpoints[endpoint.url] = endpoint
            #get all sessions running on that endpoint
            endpoint_sessions = self.spark_controller.get_all_sessions_endpoint(endpoint)
            #add each session to session manager.
            for session in endpoint_sessions:
                print(session)
                name = self.spark_controller.session_manager.get_session_name_by_id_endpoint(session.id, endpoint)
                print(name)
                self.spark_controller.session_manager.add_session(name, session)
        
        """

        widget = MagicsControllerWidget(self.spark_controller, IpyWidgetFactory(), self.ipython_display, self.endpoints)
        if self.endpoints is None:
            self.endpoints = {}
        self.manage_dataproc_widget = widget
        self.__remotesparkmagics = RemoteSparkMagics(shell, widget)

    @line_magic
    def manage_dataproc(self, line, local_ns=None):
        print(self.endpoints)
        """Magic to manage Spark endpoints and sessions for Dataproc. First, add an endpoint via the 'Add Endpoint' tab.
        Then, create a session. You'll be able to select the session created from the %%spark magic."""
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
               Display the available Livy sessions and other configurations for sessions.
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

        #should be able to take this out. 
        if args.auth is None:
            args.auth = conf.get_auth_value(args.user, args.password)
        else:
            args.auth = args.auth
        # to hhere 

        if subcommand == "add":
            if args.url is None:
                self.ipython_display.send_error("Need to supply URL argument (e.g. -u https://example.com/livyendpoint)")
                return
            name = args.session
            language = args.language
            endpoint = Endpoint(args.url, initialize_auth(args))
            self.endpoints[args.url] = endpoint
            print(endpoint)
            #get current stored_endpoints
            stored_endpoints = self.ipython.user_ns['stored_endpoints']
            # append new (url,account) tuple
            stored_endpoints.append((args.url, endpoint.auth.active_credentials))
            """
            stored_endpoints = list()

            for url, endpoint in self.endpoints.items():
                print(url)
                print(endpoint)
                stored_endpoints.append((url, endpoint.auth.active_credentials))
            
            print(stored_endpoints)
            """
            #update with stored_endpoints
            self.ipython.user_ns['stored_endpoints'] = stored_endpoints
            #call update store so that stored endpoints will get updated stored_endpoints
            self.ipython.run_line_magic('store', '-r stored_endpoints')
            print(self.ipython.user_ns['stored_endpoints'])
            skip = args.skip
            properties = conf.get_session_properties(language)
            self.spark_controller.add_session(name, endpoint, skip, properties)
        else:
            self.__remotesparkmagics.spark(line, cell="", local_ns=None)

    @staticmethod
    def _reload_endpoints():
        """Loads endpoints that were saved with %store"""
        ipython = get_ipython()
        ipython.run_line_magic('reload_ext', 'storemagic')
        ipython.run_line_magic('store', '-r')

    def get_existing_session_widgets(self, ):
        session_widgets = []
        session_widgets.append(self.ipywidget_factory.get_html(value="<br/>", width="600px"))

        client_dict = self.spark_controller.get_managed_clients()
        if len(client_dict) > 0:
            # Header
            header = self.get_session_widget("Name", "Id", "Kind", "State", False)
            session_widgets.append(header)
            session_widgets.append(self.ipywidget_factory.get_html(value="<hr/>", width="600px"))

            # Sessions
            for name, session in client_dict.items():
                session_widgets.append(self.get_session_widget(name, session.id, session.kind, session.status))

            session_widgets.append(self.ipywidget_factory.get_html(value="<br/>", width="600px"))
        else:
            session_widgets.append(self.ipywidget_factory.get_html(value="No sessions yet.", width="600px"))

        return session_widgets

    def _print_local_info(self):
        sessions_info = ["        {}".format(i) for i in self.spark_controller.get_manager_sessions_str()]
        print("""Info for running Spark:
    Sessions:
{}
    Session configs:
        {}
""".format("\n".join(sessions_info), conf.session_configs()))

def load_ipython_extension(ip):
    ip.register_magics(StoreMagics)
    ip.register_magics(RemoteSparkMagics)
    ip.register_magics(DataprocMagics)
   
