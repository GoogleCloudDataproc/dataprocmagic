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

import importlib
import ipyvuetify as v

from sparkmagic.livyclientlib.endpoint import Endpoint
import sparkmagic.utils.configuration as conf
from sparkmagic.utils.constants import WIDGET_WIDTH
from sparkmagic.controllerwidget.abstractmenuwidget import AbstractMenuWidget


class AddEndpointWidget(AbstractMenuWidget):

    def __init__(self, spark_controller, ipywidget_factory, ipython_display, endpoints, endpoints_dropdown_widget,
                 refresh_method, state, db):
                 
        # This is nested
        super(AddEndpointWidget, self).__init__(spark_controller, ipywidget_factory, ipython_display, True)
        self.endpoints = endpoints
        self.refresh_method = refresh_method
        self.state = state
        self.db = db
            
        #map auth class path string to the instance of the class.
        self.auth_instances = {}
        for auth in conf.authenticators().values():
            module, class_name = (auth).rsplit('.', 1)
            if (class_name == "GoogleAuth"):
                events_handler_module = importlib.import_module(module)
                auth_class = getattr(events_handler_module, class_name)
                self.auth_instances[auth] = auth_class()

        self.auth_type = self.ipywidget_factory.get_dropdown(
            options={u"Google": u"googledataprocauthenticator.google.GoogleAuth"},
            description=u"Auth type:"
        )

        self.auth_type.layout.display = 'none'

        #combine all authentication instance's widgets into one list to pass to self.children.
        self.all_widgets = list()
        for _class, instance in self.auth_instances.items():
            for widget in instance.widgets:
                if  _class == self.auth_type.value:
                    widget.layout.display = 'flex'
                    self.auth = instance
                else:
                    widget.layout.display = 'none'
                self.all_widgets.append(widget)

        # Submit widget
        self.add_endpoint_widget = v.Btn(class_='ma-2', color='primary', children=['Add Endpoint'])
        self.add_endpoint_widget.on_event('click', self._add_endpoint)
        self.cancel = v.Btn(class_='ma-2', color='primary', children=['Cancel'])
        self.cancel.on_event('click', self._on_cancel_click)

        backicon = v.Icon(children=['mdi-arrow-left'])
        backicon.on_event('click', self._on_back_click)
        back_toolbar = v.Toolbar(elevation="0",
            children=[
                v.ToolbarItems(children=[backicon]),
                v.ToolbarTitle(children=['Create new endpoint']),
                v.Spacer()
            ],
            app=True,  # If true, the other widgets float under on scroll
        )
        
        self.flex_widget = v.Container(style_=f'width: {WIDGET_WIDTH};', class_='ma-2', children=[
            back_toolbar,
            v.Row(class_='ma-2', children=[
                v.Col(children=[self.all_widgets[0]]),
                v.Col(children=[self.all_widgets[1]]),
                v.Col(children=[self.all_widgets[2]])
            ]),
            v.Row(class_='ma-2', children=[
                v.Col(children=[self.all_widgets[3]]),
                v.Col(children=[self.all_widgets[4]]),
                v.Col(children=[v.Spacer()]),
            ]),
            v.Row(class_='ma-2', children=[
                self.add_endpoint_widget, self.cancel])
        ])

        self.auth_type.on_trait_change(self._update_auth)
        
        endpoint_table_values = self._generate_endpoint_values()
        new_endpoint = v.Btn(class_='ma-2', color='primary', children=['New Endpoint'])
  
        new_endpoint.on_event('click', self._on_new_endpoint_click)

        no_back_toolbar = v.Toolbar(elevation="0",
            children=[
                v.ToolbarTitle(titleMarginStart='12dp',contentInsetStartWithNavigation="56dp", children=['Endpoints']),
                v.Spacer()
            ],
            app=True,  # If true, the other widgets float under on scroll
        )
        self.toolbar = v.Row(children=[no_back_toolbar, new_endpoint])

        self.endpoint_table = v.DataTable(style_=f'width: {WIDGET_WIDTH};', no_data_text = 'No endpoints', hide_default_footer=True, disable_pagination=True, item_key='name', headers=[
            {'text': 'Cluster', 'align': 'start', 'sortable': False, 'value': 'name'},
            {'text': 'Project', 'sortable': False, 'value': 'project'},
            {'text': 'Region', 'sortable': False, 'value': 'region'},
            {'text': 'Url', 'sortable': False, 'value': 'url'},
        ], items=endpoint_table_values, dense=False, fixedHeader=False)

        self.toolbar_with_table = v.Container(style_=f'width: {WIDGET_WIDTH};', class_='mx-auto', children=[
            v.Row(class_='mx-auto', children=[self.toolbar]),
            v.Row(class_='mx-auto', children=[self.endpoint_table])])

        

        self.children = [self.flex_widget, self.toolbar_with_table]
        for child in self.children:
            child.parent_widget = self
        self._update_view()
        self._update_auth()

    def _add_endpoint(self, widget, event, data):
        self.state = 'list'
        self.auth.update_with_widget_values()
        if self.auth_type.label == "None":
            endpoint = Endpoint(self.auth.url, None)
        else:
            endpoint = Endpoint(self.auth.url, self.auth)
        self.endpoints[self.auth.url] = endpoint
        stored_endpoints1 = [SerializableEndpoint(endpoint).__dict__ for endpoint in self.endpoints.values()]
        # convert self.endpoints dict into list of (url, account) tuples
        stored_endpoints = [(url, endpoint.auth.active_credentials) for url, endpoint in self.endpoints.items()]
        # stored updated stored_endpoints
        self.db['autorestore/' + 'stored_endpoints1'] = stored_endpoints1
        self.db['autorestore/' + 'stored_endpoints'] = stored_endpoints
        self.ipython_display.writeln("Added endpoint {}".format(self.auth.url))
        try:
            # We need to call the refresh method because drop down in Tab 2 for endpoints wouldn't
            # refresh with the new value otherwise.
            self.refresh_method(1)
        except:
            self.endpoints.pop(self.auth.url, None)
            self.refresh_method(1)
            raise

    def _update_view(self):
        if self.state == 'add':
            self.toolbar_with_table.layout.display = 'none'
            self.flex_widget.layout.display = 'flex'
        elif self.state == 'list':
            self.flex_widget.layout.display = 'none'
            self.toolbar_with_table.layout.display = 'flex'
    
    def _update_auth(self):
        """
        Create an instance of the chosen auth type maps to in the config file.
        """
        for widget in self.auth.widgets:
            widget.layout.display = 'none'
        self.auth = self.auth_instances.get(self.auth_type.value)

        for widget in self.auth.widgets:
            widget.layout.display = 'flex'
    
    def _on_cancel_click(self, widget, event, data):
        self.state = 'list'
        self._update_view()
    
    def _on_back_click(self, widget, event, data):
        self.state = 'list'
        self._update_view()

    def _on_new_endpoint_click(self, widget, event, data):
        self.state = 'add'
        self._update_view()

    def _generate_endpoint_values(self):
        endpoint_table_values = []
        for endpoint in self.get_stored_endpoints1():
            endpoint_table_values.append({'name':endpoint.get('cluster'), 'url':endpoint.get('url'), 'project': endpoint.get('project'), \
                'region':endpoint.get('region')})
        return endpoint_table_values

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

class SerializableEndpoint():
    def __init__(self, endpoint):
        
        self.cluster = endpoint.auth.cluster_combobox.v_model
        self.url = endpoint.url
        self.project = endpoint.auth.project_textfield.v_model
        self.region = endpoint.auth.region_combobox.v_model
        self.account = endpoint.auth.active_credentials


