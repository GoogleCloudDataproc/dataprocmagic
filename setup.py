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

import setuptools

setuptools.setup(
    name='dataprocmagic',
    python_requires='>=3',
    version='0.1',
    description='Google Dataproc authenticator for Sparkmagic',
    url='https://github.com/GoogleCloudDataproc/dataprocmagic',
    packages=setuptools.find_packages(),
    license='Apache 2.0',
    install_requires=[
        'sparkmagic==0.15.0',
        'nose',
        'mock',
        'hdijupyterutils>=0.6',
        'google-cloud-dataproc>=2.0',
        'google-auth',
        'urllib3',
        'ipyvuetify'
    ]
)
