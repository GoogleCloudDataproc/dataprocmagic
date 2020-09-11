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


"""Creates the ``%manage_dataproc widget``"""

import ipyvuetify as v

import googledataprocauthenticator.sparkmagic.utils.configuration as conf
from googledataprocauthenticator.controllerwidget.addendpointwidget import AddEndpointWidget
from googledataprocauthenticator.controllerwidget.createsessionwidget import CreateSessionWidget
from googledataprocauthenticator.sparkmagic.controllerwidget.abstractmenuwidget import AbstractMenuWidget
from googledataprocauthenticator.sparkmagic.livyclientlib.endpoint import Endpoint
from googledataprocauthenticator.sparkmagic.utils.constants import LANGS_SUPPORTED
from googledataprocauthenticator.sparkmagic.utils.utils import Namespace, initialize_auth
from googledataprocauthenticator.utils.utils import _restore_endpoints_and_sessions


class ControllerWidget(AbstractMenuWidget):
    def __init__(self, spark_controller, ipywidget_factory, ipython_display, db, endpoints=None):
        super(ControllerWidget, self).__init__(spark_controller, ipywidget_factory,
                                               ipython_display)
        if endpoints is None:
            endpoints = {
                endpoint.url: endpoint for endpoint in self._get_default_endpoints()}
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
            endpoint_config = getattr(
                conf, 'kernel_%s_credentials' %
                kernel_type)()
            if all([p in endpoint_config for p in ["url", "password",
                                                   "username"]]) and endpoint_config["url"] != "":
                user = endpoint_config["username"]
                passwd = endpoint_config["password"]
                args = Namespace(
                    user=user,
                    password=passwd,
                    auth=endpoint_config.get("auth", None))
                auth_instance = initialize_auth(args)

                default_endpoints.add(Endpoint(
                    auth=auth_instance,
                    url=endpoint_config["url"],
                    implicitly_added=True))
        return default_endpoints

    def _refresh(self, tab=0):
        _restore_endpoints_and_sessions(
            self.db, self.ipython_display, self.spark_controller, self.endpoints
        )
        self.create_session = CreateSessionWidget(
            self.spark_controller,
            self.ipywidget_factory,
            self.ipython_display,
            self.endpoints,
            self._refresh,
            self.state,
            self.db)

        self.add_endpoint = AddEndpointWidget(
            self.spark_controller,
            self.ipywidget_factory,
            self.ipython_display,
            self.endpoints,
            self._refresh,
            self.state,
            self.db)

        session_tab = [
            v.Tab(
                children=['Sessions']), v.TabItem(
                    style_='border: 1px solid lightgrey', children=[
                        self.create_session])]

        endpoint_tab = [
            v.Tab(
                children=['Endpoint']), v.TabItem(
                    style_='border: 1px solid lightgrey', children=[
                        self.add_endpoint])]

        self.tabs = v.Tabs(
            style_='border: 1px solid lightgrey',
            v_model=tab,  # loads session tab when on
            children=session_tab + endpoint_tab)

        self.children = [self.tabs]

        for child in self.children:
            child.parent_widget = self
