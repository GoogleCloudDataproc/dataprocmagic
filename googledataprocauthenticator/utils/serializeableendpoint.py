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

class SerializableEndpoint():
    """ A class that serializes an endpoint object for storing and restoring endpoints"""  
    def __init__(self, endpoint):
        self.cluster = endpoint.auth.cluster_combobox.v_model
        self.url = endpoint.url
        self.project = endpoint.auth.project_textfield.v_model
        self.region = endpoint.auth.region_combobox.v_model
        self.account = endpoint.auth.active_credentials
