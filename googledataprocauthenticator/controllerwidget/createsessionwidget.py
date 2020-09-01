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
    def __init__(self, spark_controller, ipywidget_factory, ipython_display, endpoints, endpoints_dropdown_widget, refresh_method, state):
        # This is nested
        super(CreateSessionWidget, self).__init__(spark_controller, ipywidget_factory, ipython_display, True)
        self.endpoints = endpoints
        self.refresh_method = refresh_method
        self.session_name = None
        self.properties = None
        self.language = None
        self.endpoint = None
        self.state = state
        #self.endpoints_dropdown_widget = endpoints_dropdown_widget
        # if there are no sessions, then we bring them to empty list. ?
        if not self.spark_controller.get_managed_clients():
            print('nosessions')

        self.name_textfield = v.TextField(
            class_='ma-2',
            placeholder='Enter session name',
            label='Name',
            dense=True,
            value='value',
            color='primary',
            outlined=True,
        )
        self.name_textfield.on_event('change', self._set_name)

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
        )
        
        self.endpoints_dropdown_widget.on_event('change', self._set_endpoint)
        
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
        )
    
        self.language_dropdown.on_event('change', self._set_language)


        self.properties_textbox = v.TextField(
            class_='ma-2',
            value=json.dumps(conf.session_configs()),
            label='Properties',
            dense=True,
            color='primary',
            outlined=True
        )
        
        self.properties_textbox.on_event('change', self._set_properties)

        
        self.create_session = v.Btn(class_='ma-2', color='primary', children=['Create'])
        self.cancel = v.Btn(class_='ma-2', color='primary', children=['Cancel'])


        self.create_session = v.Container(style_=f'width: {WIDGET_WIDTH};', class_='mx-auto', children=[
            v.Row(class_='mx-auto', children=[
                v.Col(cols=3, children=[self.name_textfield])
            ]),
            v.Row(class_='mx-auto', children=[
                v.Col(cols=3, children=[self.endpoints_dropdown_widget])
            ]),
            v.Row(class_='mx-auto', children=[
                v.Col(cols=3, children=[self.language_dropdown])
            ]),
            v.Row(class_='mx-auto', children=[
                v.Col(cols=3, children=[self.properties_textbox])
            ]),
            v.Row(class_='mx-auto', children=[
                v.Col( cols=4, children=[self.create_session]),
                v.Col( cols=4, children=[self.cancel])
            ])
        ])
        

        self.create_session.on_event('click', self._on_create_click)
        
        
        session_table_values = self._generate_session_values()
        new_session = v.Btn(class_='ma-2', color='primary', children=['New Session'])
        backicon = v.Icon(children=['mdi-arrow-left'])
        #backicon.on_event('click', self._on_back_click)
        new_session.on_event('click', self._on_new_session_click)

        back_toolbar = v.Toolbar(elevation="0",
            children=[
                v.ToolbarItems(children=[backicon]),
                v.ToolbarTitle(titleMarginStart='12dp',contentInsetStartWithNavigation="56dp",children=['Sessions']),
                v.Spacer()
            ],
            app=True,  # If true, the other widgets float under on scroll
        )
        self.toolbar = v.Row(children=[back_toolbar, new_session])

        self.session_table = v.DataTable(no_data_text='No sessions', hide_default_footer=True, disable_pagination=True, item_key='name', headers=[
            {'text': 'Session', 'align': 'start', 'sortable': False, 'value': 'name'},
            {'text': 'ID', 'sortable': False, 'value': 'id'},
            {'text': 'Yarn ID', 'sortable': False, 'value': 'yarn'},
            {'text': 'Spark UI', 'sortable': False, 'value': 'spark'},
            {'text': 'Status', 'sortable': False, 'value': 'status'},
            {'text': 'Kind', 'sortable': False, 'value': 'kind'},
        ], items=session_table_values, dense=False, fixedHeader=False)

        self.toolbar_with_table = v.Container(style_=f'width: {WIDGET_WIDTH};', class_='mx-auto', children=[
            v.Row(class_='mx-auto', children=[self.toolbar]),
            v.Row(class_='mx-auto', children=[self.session_table])])

        # self.children = [self.ipywidget_factory.get_html(value="<br/>", width=WIDGET_WIDTH), self.auth_type] + self.all_widgets \
        # + [self.ipywidget_factory.get_html(value="<br/>", width=WIDGET_WIDTH), self.submit_widget]
        
        # self.children = self.all_widgets.append(self.submit_widget)

        self.children = [self.create_session, self.toolbar_with_table]
        for child in self.children:
            child.parent_widget = self
        self._update_view()
        


        # self.session_widget = self.ipywidget_factory.get_text(
        #     description='Name:',
        #     value='session-name'
        # )
        # self.lang_widget = self.ipywidget_factory.get_toggle_buttons(
        #     description='Language:',
        #     options=[LANG_SCALA, LANG_PYTHON],
        # )
        # self.properties = self.ipywidget_factory.get_text(
        #     description='Properties:',
        #     value=json.dumps(conf.session_configs())
        # )
        # self.submit_widget = self.ipywidget_factory.get_submit_button(
        #     description='Create Session'
        # )

        # self.children = [self.ipywidget_factory.get_html(value="<br/>", width="600px"), self.endpoints_dropdown_widget,
        #                  self.session_widget, self.lang_widget, self.properties,
        #                  self.ipywidget_factory.get_html(value="<br/>", width="600px"), self.submit_widget]

        # for child in self.children:
        #     child.parent_widget = self

    #def run(self):
    def _on_create_click(self, widget, event, data):
        try:
            properties_json = self.properties_textbox.value
            if properties_json.strip() != "":
                conf.override(conf.session_configs.__name__, json.loads(self.properties.value))
        except ValueError as e:
            self.ipython_display.send_error("Session properties must be a valid JSON string. Error:\n{}".format(e))
            return

        endpoint = self.endpoint
        language = self.language
        alias = self.session_name
        skip = False
        properties = conf.get_session_properties(language)

        try:
            self.spark_controller.add_session(alias, endpoint, skip, properties)
        except ValueError as e:
            self.ipython_display.send_error("""Could not add session with
name:
    {}
properties:
    {}

due to error: '{}'""".format(alias, properties, e))
            return

        self.refresh_method()

    def _set_name(self, widget, event, data):
        self.session_name = data
    
    def _set_properties(self, widget, event, data):
        self.properties = data
    
    def _set_endpoint(self, widget, event, data):
        self.endpoint = data
    
    def _set_language(self, widget, event, data):
        self.language = data

    def _on_new_session_click(self, widget, event, data):
        self.state = 'add'
        self._update_view()
    
    def _on_cancel_click(self, widget, event, data):
        self.state = 'list'
        self._update_view()

    def _generate_session_values(self):
        session_table_values = []
        for name, session in self.spark_controller.get_managed_clients().items():
            #need a way to list endpoint 
            #return u"Session id: {}\tYARN id: {}\tKind: {}\tState: {}\n\tSpark UI: {}\n\tDriver Log: {}"\
            #.format(self.id, self.get_app_id(), self.kind, self.status, self.get_spark_ui_url(), self.get_driver_log_url())
            session_table_values.append({'name':name, 'id':session.id, 'yarn': session.get_app_id(), 'spark': session.get_spark_ui_url(), \
               'status':session.status,'kind':session.kind })
        return session_table_values

    def _update_view(self):
        if self.state == 'add':
            self.toolbar_with_table.layout.display = 'none'
            self.create_session.layout.display = 'flex'
        elif self.state == 'list':
            self.create_session.layout.display = 'none'
            self.toolbar_with_table.layout.display = 'flex'