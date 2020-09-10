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


"""Helper functions for commonly used utilities."""


from sparkmagic.livyclientlib.endpoint import Endpoint
from sparkmagic.livyclientlib.exceptions import BadUserConfigurationException
from sparkmagic.utils.utils import initialize_auth, Namespace

class SerializableEndpoint():
    """ A class that serializes an endpoint object for storing and restoring endpoints"""
    def __init__(self, endpoint):
        self.cluster = endpoint.auth.cluster_widget.v_model
        self.url = endpoint.url
        self.project = endpoint.auth.project_widget.v_model
        self.region = endpoint.auth.region_widget.v_model
        self.account = endpoint.auth.active_credentials

def get_stored_endpoints(db, ipython_display):
    """Gets a list of endpoints that were added in previous notebook sessions

    Args:
        db (dict): the ipython database where stored_endpoints list will be stored
        ipython_display (hdijupyterutils.ipythondisplay.IpythonDisplay): the display that
        informs the user of any errors that occur while restoring endpoints

    Returns:
        stored_endpoints (Sequence[dict]): A list of dicts, each dict storing an
        Endpoint's writeable attributes. If no endpoints can be obtained from previous
        notebook sessions, an empty list is returned.
    """
    try:
        stored_endpoints = db['autorestore/' + 'stored_endpoints']
        return stored_endpoints
    except KeyError:
        db['autorestore/' + 'stored_endpoints'] = list()
        return list()
    except Exception as caught_exc:
        db['autorestore/' + 'stored_endpoints'] = list()
        ipython_display.writeln("Failed to restore stored_endpoints from a previous notebook "\
                        f"session due to an error: {str(caught_exc)}. Cleared stored_endpoints.")
        return list()

def get_session_id_to_name(db, ipython_display):
    """Gets a dictionary that maps currently running livy session id's to their names

    Args:
        db (dict): the ipython database where sessions dict will be stored
        ipython_display (hdijupyterutils.ipythondisplay.IpythonDisplay): the display that
        informs the user of any errors that occur while restoring sessions

    Returns:
        session_id_to_name (dict): a dictionary mapping session.id -> name
        If no sessions can be obtained from previous notebook sessions, an
        empty dict is returned.
    """
    try:
        session_id_to_name = db['autorestore/' + 'session_id_to_name']
        return session_id_to_name
    except KeyError:
        db['autorestore/' + 'session_id_to_name'] = dict()
        return dict()
    except Exception as caught_exc:
        ipython_display.writeln("Failed to restore session_id_to_name from a previous notebook "\
                        f"session due to an error: {str(caught_exc)}. Cleared session_id_to_name.")
        return dict()

def _restore_endpoints_and_sessions(db, ipython_display, spark_controller, endpoints):
    """Loads all of the running livy sessions of an endpoint

    Args:
        db (dict): the ipython database where stored_endpoints list will be stored
        ipython_display (hdijupyterutils.ipythondisplay.IpythonDisplay): the display that
        informs the user of any errors that occur while restoring endpoints
        spark_controller (sparkmagic.livyclientlib.sparkcontroller.SparkController): an object that
        manages all the spark sessions
        endpoints (dict): the endpoints dict that restored endpoints will be added to.
    """
    stored_endpoints = get_stored_endpoints(db, ipython_display)
    try:
        for serialized_endpoint in stored_endpoints:
            args = Namespace(auth='Google', url=serialized_endpoint.get('url'), \
                account=serialized_endpoint.get('account'))
            auth = initialize_auth(args)
            endpoint = Endpoint(url=serialized_endpoint.get('url'), auth=auth)
            endpoints[endpoint.url] = endpoint

    # If a user revokes the credentials used for stored endpoints and sessions,
    # all of the stored endpoints and sessions are cleared.
    except BadUserConfigurationException as caught_exc:
        db['autorestore/' + 'stored_endpoints'] = list()
        db['autorestore/' + 'session_id_to_name'] = dict()
        ipython_display.send_error("Failed to restore endpoints and sessions "\
                    f"due to an authentication error: {str(caught_exc)}. "\
                        "Cleared stored_endpoints and session_id_to_name.")

    for endpoint in endpoints.values():
        session_id_to_name = get_session_id_to_name(db, ipython_display)
        #get all sessions running on that endpoint
        endpoint_sessions = spark_controller.get_all_sessions_endpoint(endpoint)
        #add each session to session manager.
        for session in endpoint_sessions:
            name = session_id_to_name.get(session.id)
            if name is not None and name not in spark_controller.get_managed_clients():
                spark_controller.session_manager.add_session(name, session)
