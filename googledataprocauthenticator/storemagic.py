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
from sparkmagic.controllerwidget.abstractmenuwidget import MagicsControllerWidget
from sparkmagic.controllerwidget.addendpointwidget import AddEndpointWidget
from sparkmagic.livyclientlib.endpoint import Endpoint


from IPython import get_ipython

class RestoreMagic(MagicsControllerWidget):
    """Adds reload_endpoints function to MagicsControllerWidget"""

    def __init__(self, spark_controller, ipywidget_factory, ipython_display, endpoints=None):
        if endpoints is None:
            endpoints = {endpoint.url: endpoint for endpoint in self._get_default_endpoints()}
            endpoints.update(self._reload_endpoints())
        super(RestoreMagic, self).__init__(spark_controller, ipywidget_factory, ipython_display, endpoints)

    @staticmethod
    def _reload_endpoints():
        """Loads endpoints that were saved with %store"""
        ipython = get_ipython()
        ipython.run_line_magic('load_ext', 'storemagic')
        ipython.run_line_magic('store', '-r')
        return ipython.user_ns


class StoreMagic(AddEndpointWidget):
    """Overrides run function in AddEndpointWidget"""
    def __init__(self, spark_controller, ipywidget_factory, ipython_display, endpoints, endpoints_dropdown_widget,
                 refresh_method):
        # This is nested
        super(StoreMagic, self).__init__(spark_controller, ipywidget_factory, ipython_display, endpoints, \
            endpoints_dropdown_widget, refresh_method)
    
    def run(self):
        ipython = get_ipython()
        self.auth.update_with_widget_values()
        if self.auth_type.label == "None":
            endpoint = Endpoint(self.auth.url, None)
        else:
            endpoint = Endpoint(self.auth.url, self.auth)
        self.endpoints[self.auth.url] = endpoint
        ipython.user_ns[self.auth.url] = endpoint
        ipython.run_line_magic('store', self.auth.url)
        self.ipython_display.writeln("Added endpoint {}".format(self.auth.url))
        try:
            # We need to call the refresh method because drop down in Tab 2 for endpoints wouldn't
            # refresh with the new value otherwise.
            self.refresh_method()
        except:
            self.endpoints.pop(self.auth.url, None)
            ipython.run_line_magic('store', f"-d {self.auth.url}")
            self.refresh_method()
            raise

# but this will not work because then whereever Sparkmagic uses these functions, I would have to do like 
# AddEndpointWidget.run() = Storemagic.run. How would I override these functions without changing sparkmagic?  

#would also have to override delete_endpoint to delete the endpoint.url key from user_ns
"""
    def get_delete_session_endpoint_widget(self, url, endpoint):
        session_text = self.ipywidget_factory.get_text(description="Session to delete:", value="0", width="50px")

        def delete_endpoint(button):
            try:
                id = session_text.value
                self.spark_controller.delete_session_by_id(endpoint, id)
                self.ipython_display.writeln("Deleted session {} at {}".format(id, url))
            except ValueError as e:
                self.ipython_display.send_error(str(e))
                return
            self.refresh_method()

        button = self.ipywidget_factory.get_button(description="Delete")
        button.on_click(delete_endpoint)

"""