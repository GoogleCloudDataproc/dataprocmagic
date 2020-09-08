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


"""Creates the widget under the Sessions tab within the ``%manage_dataproc widget``"""

import importlib
from sparkmagic.livyclientlib.endpoint import Endpoint
import sparkmagic.utils.configuration as conf
from sparkmagic.controllerwidget.abstractmenuwidget import AbstractMenuWidget
import ipyvuetify as v
from googledataprocauthenticator.utils.utils import SerializableEndpoint, get_stored_endpoints
from googledataprocauthenticator.utils.constants import WIDGET_WIDTH


class AddEndpointWidget(AbstractMenuWidget):

    def __init__(self, spark_controller, ipywidget_factory, ipython_display, endpoints,
                 refresh_method, state, db):
        super(AddEndpointWidget, self).__init__(
            spark_controller, ipywidget_factory, ipython_display, True)
        self.endpoints = endpoints
        self.refresh_method = refresh_method
        self.state = state
        self.delete_pressed = False
        self.db = db

        auth = conf.authenticators().get("Google")
        module, class_name = (auth).rsplit('.', 1)
        events_handler_module = importlib.import_module(module)
        auth_class = getattr(events_handler_module, class_name)
        self.auth = auth_class()

        add_endpoint_button = v.Btn(class_='ma-2', color='primary', children=['Add Endpoint'])
        add_endpoint_button.on_event('click', self._add_endpoint)
        cancel_button = v.Btn(class_='ma-2', color='primary', children=['Cancel'])
        cancel_button.on_event('click', self._on_cancel_click)

        backicon = v.Icon(children=['mdi-arrow-left'])
        backicon.on_event('click', self._on_back_click)
        back_toolbar = v.Toolbar(
            elevation="0",
            children=[
                v.ToolbarItems(children=[backicon]),
                v.ToolbarTitle(children=['Create new endpoint']),
                v.Spacer()
                ],
            app=True,  # If true, the other widgets float under on scroll
            )

        self.create_endpoint_widget = v.Container(
            style_=f'width: {WIDGET_WIDTH};', class_='ma-2',
            children=[
                back_toolbar,
                v.Row(class_='ma-2', children=[
                    v.Col(children=[self.auth.account_widget]),
                    v.Col(children=[self.auth.project_widget]),
                    v.Col(children=[self.auth.region_widget])
                ]),
                v.Row(class_='ma-2', children=[
                    v.Col(children=[self.auth.cluster_widget]),
                    v.Col(children=[self.auth.filter_widget]),
                    v.Col(children=[v.Spacer()]),
                ]),
                v.Row(class_='ma-2', children=[
                    add_endpoint_button, cancel_button])
            ]
        )

        endpoint_table_values = self._generate_endpoint_values()
        new_endpoint = v.Btn(class_='ma-2', color='primary', children=['New Endpoint'])
        new_endpoint.on_event('click', self._on_new_endpoint_click)

        no_back_toolbar = v.Toolbar(
            elevation="0",
            children=[
                v.ToolbarTitle(titleMarginStart='12dp',
                               contentInsetStartWithNavigation="56dp", children=['Endpoints']),
                v.Spacer()
            ],
            app=True,  # If true, the other widgets float under on scroll
        )
        toolbar = v.Row(children=[no_back_toolbar, new_endpoint])
        delete_icon = v.Icon(children=['mdi-delete'])
        delete_icon.on_event('click', self._on_delete_icon_pressed)

        endpoint_table = v.DataTable(
            style_=f'width: {WIDGET_WIDTH};', no_data_text='No endpoints',
            hide_default_footer=True, disable_pagination=True, item_key='url', headers=[
                {'text': 'Cluster', 'align': 'start', 'sortable': False, 'value': 'name'},
                {'text': 'Project', 'sortable': False, 'value': 'project'},
                {'text': 'Region', 'sortable': False, 'value': 'region'},
                {'text': 'Account', 'sortable': False, 'value': 'account'},
                {'text': 'Url', 'sortable': False, 'value': 'url'},
                {'text': '', 'sortable': False, 'value': 'actions'},
            ],
            items=endpoint_table_values, dense=False, v_slots=[
                {'name': 'item.actions', 'children': [delete_icon]},
                {'name': 'no-data', 'children': ['No endpoints']}
            ]
        )
        endpoint_table.on_event('click:row', self._remove_row_from_table)

        self.toolbar_with_table = v.Container(
            style_=f'width: {WIDGET_WIDTH};', class_='mx-auto', children=[
                v.Row(class_='mx-auto', children=[toolbar]),
                v.Row(class_='mx-auto', children=[endpoint_table])
            ]
        )

        self.children = [self.create_endpoint_widget, self.toolbar_with_table]
        for child in self.children:
            child.parent_widget = self
        self._update_view()

    def run(self):
        pass

    def _add_endpoint(self, _widget, _event, _data):
        self.state = 'list'
        self.auth.update_with_widget_values()
        endpoint = Endpoint(self.auth.url, self.auth)
        self.endpoints[self.auth.url] = endpoint
        # convert Endpoints in self.endpoints into list of dictionaries, each storing an Endpoints
        # writeable attributes
        stored_endpoints = [SerializableEndpoint(
            endpoint).__dict__ for endpoint in self.endpoints.values()]
        # stored updated stored_endpoints
        self.db['autorestore/' + 'stored_endpoints'] = stored_endpoints
        self.ipython_display.writeln("Added endpoint {}".format(self.auth.url))
        try:
            self.refresh_method(1)
        except:
            self.endpoints.pop(self.auth.url, None)
            self.refresh_method(1)
            raise

    def _update_view(self):
        if self.state == 'add':
            self.toolbar_with_table.layout.display = 'none'
            self.create_endpoint_widget.layout.display = 'flex'
        elif self.state == 'list':
            self.create_endpoint_widget.layout.display = 'none'
            self.toolbar_with_table.layout.display = 'flex'

    def _remove_row_from_table(self, _table, _event, row):
        if self.delete_pressed:
            endpoint_url = row.get('url')
            try:
                self.endpoints.pop(endpoint_url)
                stored_endpoints = [SerializableEndpoint(
                    endpoint).__dict__ for endpoint in self.endpoints.values()]
                # stored updated stored_endpoints
                self.db['autorestore/' + 'stored_endpoints'] = stored_endpoints
                self.refresh_method(1)
            except Exception as caught_exc:
                self.ipython_display.send_error("Failed delete session due to the following "\
                    f"error: {str(caught_exc)}")

    def _on_cancel_click(self, _widget, _event, _data):
        self.state = 'list'
        self._update_view()

    def _on_back_click(self, _widget, _event, _data):
        self.state = 'list'
        self._update_view()

    def _on_new_endpoint_click(self, _widget, _event, _data):
        self.state = 'add'
        self._update_view()

    def _on_delete_icon_pressed(self, _widget, _event, _data):
        self.delete_pressed = True

    def _generate_endpoint_values(self):
        endpoint_table_values = []
        for endpoint in get_stored_endpoints(self.db, self.ipython_display):
            endpoint_table_values.append({
                'name':endpoint.get('cluster'),
                'url':endpoint.get('url'),
                'project': endpoint.get('project'),
                'region':endpoint.get('region'),
                'account':endpoint.get('account')
            })
        return endpoint_table_values
