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

# Copyright (c) 2015  aggftw@gmail.com
# Distributed under the terms of the Modified BSD License.
import json
import ipyvuetify as v

import sparkmagic.utils.configuration as conf
from sparkmagic.utils.constants import LANG_SCALA, LANG_PYTHON
from sparkmagic.controllerwidget.abstractmenuwidget import AbstractMenuWidget
from sparkmagic.utils.constants import WIDGET_WIDTH



class CreateSessionWidget(AbstractMenuWidget):
    def __init__(self, spark_controller, ipywidget_factory, ipython_display, endpoints, endpoints_dropdown_widget, refresh_method, state, db):
        # This is nested
        super(CreateSessionWidget, self).__init__(spark_controller, ipywidget_factory, ipython_display, True)
        self.endpoints = endpoints
        self.refresh_method = refresh_method
        self.properties = json.dumps(conf.session_configs())
     
        self.state = state
        self.db = db
        self.delete_pressed = False
        backicon = v.Icon(children=['mdi-arrow-left'])
        backicon.on_event('click', self._on_back_click)
        back_toolbar = v.Toolbar(elevation="0",
            children=[
                v.ToolbarItems(children=[backicon]),
                v.ToolbarTitle(children=['Create new session']),
                v.Spacer()
            ],
            app=True,  # If true, the other widgets float under on scroll
        )

        self.name_textfield = v.TextField(
            class_='ma-2',
            placeholder='Enter session name',
            label='Name',
            dense=True,
            color='primary',
            outlined=True,
            v_model = None,
        )

        self.endpoints_dropdown_widget = v.Combobox(
            class_='ma-2',
            placeholder='Select an endpoint',
            label='Endpoint',
            dense=True,
            color='primary',
            persistent_hint=True,
            hide_selected=True,
            outlined=True,
            items=list(self.endpoints.keys()),
            auto_select_first=True,
            v_model=None,
        )
        
        self.language_dropdown = v.Combobox(
            class_='ma-2',
            label='Language',
            dense=True,
            color='primary',
            persistent_hint=True,
            hide_selected=True,
            outlined=True,
            items=[LANG_SCALA, LANG_PYTHON],
            auto_select_first=True,
            v_model=None,
        )
    
        self.properties_textbox = v.TextField(
            class_='ma-2',
            label='Properties',
            dense=True,
            color='primary',
            outlined=True,
            v_model=json.dumps(conf.session_configs()),
        )
        self.create_session = v.Btn(class_='ma-2', color='primary', children=['Create'])
        self.create_session.on_event('click', self._on_create_click)
        self.cancel = v.Btn(class_='ma-2', color='primary', children=['Cancel'])
        self.cancel.on_event('click', self._on_cancel_click)

        self.create_session_container = v.Container(style_=f'width: {WIDGET_WIDTH};', class_='ma-2', children=[
            back_toolbar,
            v.Row(class_='ma-2', children=[
                v.Col(children=[self.name_textfield])
            ]),
            v.Row(class_='ma-2', children=[
                v.Col(children=[self.endpoints_dropdown_widget])
            ]),
            v.Row(class_='ma-2', children=[
                v.Col(children=[self.language_dropdown])
            ]),
            v.Row(class_='ma-2', children=[
                v.Col(children=[self.properties_textbox])
            ]),
            v.Row(class_='ma-2', children=[self.create_session,self.cancel]),
        ])
        
        
        no_back_toolbar = v.Toolbar(elevation="0",
            children=[
                v.ToolbarTitle(titleMarginStart='12dp',contentInsetStartWithNavigation="56dp", children=['Sessions']),
                v.Spacer()
            ],
            app=True,  # If true, the other widgets float under on scroll
        )
        new_session = v.Btn(class_='ma-2', color='primary', children=['New Session'])
        new_session.on_event('click', self._on_new_session_click)
        self.toolbar = v.Row(children=[no_back_toolbar, new_session])

        session_table_values = self._generate_session_values()
        self.delete_icon = v.Icon(children=['mdi-delete'])
        self.delete_icon.on_event('click', self._on_delete_icon_pressed)

        self.session_table = v.DataTable(style_=f'width: {WIDGET_WIDTH};', no_data_text='No sessions', hide_default_footer=True, disable_pagination=True, item_key='name', headers=[
            {'text': 'Session', 'align': 'start', 'sortable': False, 'value': 'name'},
            {'text': 'ID', 'sortable': False, 'value': 'id'},
            {'text': 'Status', 'sortable': False, 'value': 'status'},
            {'text': 'Kind', 'sortable': False, 'value': 'kind'},
            {'text': '', 'sortable': False, 'value': 'actions'},
        ], items=session_table_values, dense=False, fixedHeader=False, v_slots=[{
            'name': 'item.actions', 
            'children' : [ self.delete_icon]
        }, {'name': 'no-data', 'children': ['No sessions']}])
        self.session_table.on_event('click:row', self._remove_row_from_table)

        self.toolbar_with_table = v.Container(style_=f'width: {WIDGET_WIDTH};', class_='mx-auto', children=[
            v.Row(class_='mx-auto', children=[self.toolbar]),
            v.Row(class_='mx-auto', children=[self.session_table])])

        self.children = [self.create_session_container, self.toolbar_with_table]
        for child in self.children:
            child.parent_widget = self
        self._update_view()
        
    def _on_create_click(self, widget, event, data):
        try:
            properties_json = self.properties_textbox.v_model
            if properties_json.strip() != "":
                conf.override(conf.session_configs.__name__, json.loads(self.properties_textbox.v_model))
        except ValueError as e:
            self.ipython_display.send_error("Session properties must be a valid JSON string. Error:\n{}".format(e))
            return

        endpoint = self.endpoints[self.endpoints_dropdown_widget.v_model]
        language = self.language_dropdown.v_model
        alias = self.name_textfield.v_model
        skip = False
        properties = conf.get_session_properties(language)
        try:
            self.spark_controller.add_session(alias, endpoint, skip, properties)
            # session_id_to_name dict is necessary to restore session name across notebook sessions
            # since the livy server does not store the name.
            session_id_to_name = self.get_session_id_to_name()
            # add session id -> name to session_id_to_name dict

            session_id_to_name[self.spark_controller.session_manager.get_session(alias).id] = alias
            self.db[ 'autorestore/' + 'session_id_to_name'] = session_id_to_name
        except ValueError as e:
            self.ipython_display.send_error("""Could not add session with
name:
    {}
properties:
    {}

due to error: '{}'""".format(alias, properties, e))
            return

        self.refresh_method(0)
   
    def _on_delete_icon_pressed(self, widget, event, data):
        print('icon')
        self.delete_pressed = True

        print(widget)
        print(event)
        print(data)

    def _remove_row_from_table(self, table, event, row):
        if self.delete_pressed: 
            session_name = row.get('name')
            session_id = row.get('id')
            try:
                print(f"session name {session_name}")
                self.spark_controller.delete_session_by_name(session_name)
                session_id_to_name = self.get_session_id_to_name()
                session_id_to_name.pop(session_id)
                self.db[ 'autorestore/' + 'session_id_to_name'] = session_id_to_name
                self.refresh_method(0)    
            except Exception as caught_exc: 
                self.ipython_display.send_error("Failed delete session due to the following error: "\
                    f"{str(caught_exc)}")
        

        print(table)
        print(event)
        print(row)

    def _on_cancel_click(self, widget, event, data):
        self.state = 'list'
        self._update_view()


    def _on_new_session_click(self, widget, event, data):
        self.state = 'add'
        self._update_view()
    
    def _on_back_click(self, widget, event, data):
        self.state = 'list'
        self._update_view()

    def _generate_session_values(self):
        session_table_values = []
        #print('generate sessions')
        #print(self.spark_controller.get_managed_clients())
        #for name, session in self.spark_controller.get_managed_clients().items():
        #need to reload before.
        #print(self.endpoints)
        # for endpoint in self.endpoints.values():
        #     self._load_sessions_for_endpoint(endpoint)

        # print(self.spark_controller.get_managed_clients())
        print(f"generating table values with sparkcontroller.get_managed_clients: {self.spark_controller.get_managed_clients()}")
        for name, session in self.spark_controller.get_managed_clients().items():
            
            #need a way to list endpoint 
            #return u"Session id: {}\tYARN id: {}\tKind: {}\tState: {}\n\tSpark UI: {}\n\tDriver Log: {}"\
            #.format(self.id, self.get_app_id(), self.kind, self.status, self.get_spark_ui_url(), self.get_driver_log_url())
            session_table_values.append({'name':name, 'id':session.id, \
               'status':session.status,'kind':session.kind})
        return session_table_values

    def _update_view(self):
        if self.state == 'add':
            self.toolbar_with_table.layout.display = 'none'
            self.create_session_container.layout.display = 'flex'
        elif self.state == 'list':
            self.create_session_container.layout.display = 'none'
            self.toolbar_with_table.layout.display = 'flex'

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
            endpoint (sparkmagic.livyclientlib.Endpoint): a tuple of two strings in the format (url, account) where url is
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



class SerializableEndpoint():
    def __init__(self, endpoint):
        
        self.cluster = endpoint.auth.cluster_combobox.v_model
        self.url = endpoint.url
        self.project = endpoint.auth.project_textfield.v_model
        self.region = endpoint.auth.region_combobox.v_model
        self.account = endpoint.auth.active_credentials


