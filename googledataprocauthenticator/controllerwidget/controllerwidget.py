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

import ipyvuetify as v
from sparkmagic.controllerwidget.abstractmenuwidget import AbstractMenuWidget
from googledataprocauthenticator.controllerwidget.addendpointwidget import AddEndpointWidget
from googledataprocauthenticator.utils.serializeableendpoint import SerializableEndpoint
from googledataprocauthenticator.controllerwidget.createsessionwidget import CreateSessionWidget
from sparkmagic.controllerwidget.manageendpointwidget import ManageEndpointWidget
from sparkmagic.controllerwidget.managesessionwidget import ManageSessionWidget
from sparkmagic.livyclientlib.endpoint import Endpoint
from sparkmagic.utils.constants import LANGS_SUPPORTED
import sparkmagic.utils.configuration as conf
from sparkmagic.utils.utils import Namespace, initialize_auth


class ControllerWidget(AbstractMenuWidget):
    def __init__(self, spark_controller, ipywidget_factory, ipython_display, db, endpoints=None):
        super(ControllerWidget, self).__init__(spark_controller, ipywidget_factory, ipython_display)
        
        if endpoints is None:
            endpoints = {endpoint.url: endpoint for endpoint in self._get_default_endpoints()}
        self.endpoints = endpoints

        self.state = 'list'
        self.db = db
        self._refresh()

    def run(self):
        pass

    @staticmethod
    def _get_default_endpoints():
        default_endpoints = set()

        for kernel_type in LANGS_SUPPORTED:
            endpoint_config = getattr(conf, 'kernel_%s_credentials' % kernel_type)()
            if all([p in endpoint_config for p in ["url", "password", "username"]]) and endpoint_config["url"] != "":
                user = endpoint_config["username"]
                passwd = endpoint_config["password"]
                args = Namespace(user=user, password=passwd, auth=endpoint_config.get("auth", None))
                auth_instance = initialize_auth(args)

                default_endpoints.add(Endpoint(
                    auth=auth_instance,
                    url=endpoint_config["url"],
                    implicitly_added=True))

        return default_endpoints

    def _refresh(self, tab=0):
        self.endpoints_dropdown_widget = self.ipywidget_factory.get_dropdown(
            description="Endpoint:",
            options=self.endpoints
        )
        
        stored_endpoints1 = self.get_stored_endpoints1()

        for serialized_endpoint in stored_endpoints1:
            args = Namespace(auth='Google', url=serialized_endpoint.get('url'), account=serialized_endpoint.get('account'))
            auth = initialize_auth(args)
            endpoint = Endpoint(url=serialized_endpoint.get('url'), auth=auth)
            self.endpoints[endpoint.url] = endpoint
            self._load_sessions_for_endpoint(endpoint)

        self.create_session = CreateSessionWidget(self.spark_controller, self.ipywidget_factory, self.ipython_display,
                                                  self.endpoints, self.endpoints_dropdown_widget, self._refresh, self.state, self.db)
        self.add_endpoint = AddEndpointWidget(self.spark_controller, self.ipywidget_factory, self.ipython_display,
                                              self.endpoints, self.endpoints_dropdown_widget, self._refresh, self.state, self.db)


        session_tab = [v.Tab(children=['Sessions']), v.TabItem(style_='border: 1px solid lightgrey', children=[self.create_session])]
        

        endpoint_tab = [v.Tab(children=['Endpoint']), v.TabItem(style_='border: 1px solid lightgrey', children=[self.add_endpoint])]

        self.tabs = v.Tabs(
            style_='border: 1px solid lightgrey',
            v_model=tab, #loads session tab when on
            children=session_tab + endpoint_tab)

        self.children = [self.tabs]

        for child in self.children:
            child.parent_widget = self

    def get_stored_endpoints1(self):
            """Gets a list of endpoints that were added in previous notebook sessions

            Returns:
                stored_endpoints (Sequence[tuple]): A list of tuples with two str values
                (url, account) where url is an endpoint url and account is the credentialed
                account used to authenticate the endpoint connection. If no endpoints can be
                obtained from previous notebook sessions, an empty list is returned.
            """
            try:
                stored_endpoints1 = self.db['autorestore/' + 'stored_endpoints1']
                return stored_endpoints1
            except Exception as caught_exc:
                self.db['autorestore/' + 'stored_endpoints1'] = list()
                self.ipython_display.send_error("Failed to restore stored_endpoints from a previous "\
                f"notebook session due to an error: {str(caught_exc)}. Cleared stored_endpoints1.")
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


    def _load_sessions_for_endpoint(self, endpoint):
        """Loads all of the running livy sessions of an endpoint

        Args:
            endpoint_tuple (tuple): a tuple of two strings in the format (url, account) where url is
            the endpoint url and account is the credentialed account used to authenticate
        """
        
        session_id_to_name = self.get_session_id_to_name()
        #get all sessions running on that endpoint
        endpoint_sessions = self.spark_controller.get_all_sessions_endpoint(endpoint)
        #add each session to session manager.
        for session in endpoint_sessions:
            name = session_id_to_name.get(session.id)
            if name is not None and name not in self.spark_controller.get_managed_clients():
                self.spark_controller.session_manager.add_session(name, session)