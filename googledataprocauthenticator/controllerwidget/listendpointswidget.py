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


class ListEndpointsWidget(AbstractMenuWidget):

    def __init__(self, spark_controller, ipywidget_factory, ipython_display, endpoints, endpoints_dropdown_widget,
                 refresh_method):
        # This is nested
        super(ListEndpointsWidget, self).__init__(spark_controller, ipywidget_factory, ipython_display, True)
        self.endpoints = endpoints
        self.endpoints_dropdown_widget = endpoints_dropdown_widget
        self.refresh_method = refresh_method

        

        # Submit widget
        self.submit_widget = v.Btn(class_='ma-2', color='primary', children=['Add Endpoint'])
        # self.submit_widget = self.ipywidget_factory.get_submit_button(
        #     description='Add endpoint'
        # )
        desserts= [
          {
            'name': 'clane',
            'project': 'region',
            'region': 'frenka',
            'labels': 'dmsakl',
            'url': 'dnskl'
          }]
        endpoint_table_values = []
        for endpoint in self.endpoints:
            endpoint_table_values.append({'name':endpoint.auth.cluster_selection, 'url':endpoint.url, 'project': endpoint.auth.project, \
                'region':endpoint.auth.region })

        self.endpoint_table = v.DataTable(disable_pagination=True, item_key='name', headers=[
            {
                'text': 'Cluster',
                'align': 'start',
                'sortable': False,
                'value': 'name',
            },
            { 
                'text': 'Project',
                'sortable': False, 
                'value': 'project'
            },
            {
                'text': 'Region',
                'sortable': False, 
                'value': 'region' 
            },
            { 
                'text': 'Url',
                'sortable': False,
                'value': 'url'
            },
        ],items=endpoint_table_values, dense=False, fixedHeader=False)

        self.children = [self.endpoint_table]
        for child in self.children:
            child.parent_widget = self
