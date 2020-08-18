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


with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="dataprocmagic",
    python_requires='>=2.7.0',
    version="0.1.0",
    author="Alexis Macaskill",
    author_email="amacaskill@google.com",
    description="Google Dataproc authenticator for Sparkmagic",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/GoogleCloudDataproc/dataprocmagic.git",
    packages=['googledataprocauthenticator'],
    license='Apache 2.0',
    classifiers=[
        'Development Status :: 4 - Beta',
        'Environment :: Console',
        'Intended Audience :: Science/Research',
        'License :: OSI Approved :: BSD License',
        'Natural Language :: English',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7'],
    install_requires=[
        "sparkmagic>=0.15.0",
        'nose',
        'mock',
        'hdijupyterutils>=0.6',
        'google-cloud-dataproc',
        'google-auth',
        'urllib3'
    ]
)
