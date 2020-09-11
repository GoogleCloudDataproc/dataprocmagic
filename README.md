# DataprocMagic

Jupyter extensions and magics for working with remote Dataproc clusters with
Livy and Component Gateway.

## Before you begin

In order to use this library, you first need to go through the following steps:

1. [Select or create a Cloud Platform project][create_project]
2. [Enable billing for your project][enable_billing]
3. [Enable the Google Cloud Dataproc API][enable_api]
4. [Setup Authentication][authentication]

[create_project]: https://console.cloud.google.com/project
[enable_billing]: https://cloud.google.com/billing/docs/how-to/modify-project#enable_billing_for_a_project
[enable_api]: https://cloud.google.com/dataproc
[authentication]: https://cloud.google.com/docs/authentication/getting-started#auth-cloud-implicit-python


## Installation

To install into a Jupyter notebook running locally:

1. Install the google-cloud-dataproc Cloud Client Libraries.

    ```bash
    pip install google-cloud-dataproc==2.0.0 --force-reinstall --no-dependencies
    ```

1. [Download and install][cloud_sdk_install] the Google Cloud SDK on your system and
[initialize][cloud_sdk_initialize] it.

1. Install the google-cloud-dataproc Cloud Client Libraries.

    ```bash
    pip install google-cloud-dataproc==2.0.0 --force-reinstall --no-dependencies
    ```

1. Install this repository locally.

    ```bash
    pip install git+https://github.com/GoogleCloudDataproc/dataprocmagic.git#egg=dataprocmagic
    ```

1. Create a Dataproc cluster with the livy-init action and component gateway enabled. With the
gcloud command line interface.

    ```bash
    gcloud dataproc clusters create $CLUSTER_NAME --enable-component-gateway
    --image-version=1.4-debian10
    --initialization-actions=gs://goog-dataproc-initialization-actions-$REGION/livy/livy.sh
    --region $REGION
    ```

1. Install Jupyter and JupyterLab.

    ```bash
    pip install jupyter
    pip install jupyterlab
    ```

1. Run these commands to enable widgets:

    ```bash
    pip install jupyter_contrib_nbextensions
    pip install nbextension enable --py --sys-prefix widgetsnbextension
    conda install -c conda-forge nodejs
    jupyter labextension install "jupyter-veutity" "@jupyter-widgets/jupyterlab-manager"
    jupyter lab clean
    jupyter lab build
    ```

## Load the extension

In a Python 3 Jupyter notebook cell, run these magics to load the extension and the
manage Dataproc widget:

```bash
%load_ext googledataprocauthenticator.magics
%manage_dataproc
```

## Useage

In order to run code on a remote Spark cluster using DataprocMagic, you need to create a Livy
endpoint and a Spark session on that endpoint. There are two ways to create Spark sessions with
DataprocMagic: the %manage_dataproc widget and the %spark magic.

### %manage_dataproc widget

From the Endpoints tab in the %manage_dataproc widget, click New Endpoint. Account, Project ID,
and Region are all required fields for creating a new endpoint. Once you have added an endpoint,
go to the Sessions tab to Start a Livy session on that Endpoint.

#### Account

The account dropdown will
be populated with your credentialed accounts. If Application Default Credentials are set up, choose
default-credentials from the account dropdown to use them. Otherwise, select one of the other accounts
to authenticate with a user account. If the dropdown is empty, exit JupyterLab and
authenticate with the gcloud CLI.

To authenticate with a user account:
```bash
gcloud auth login
```

To authenticate with application default credentials:
```bash
gcloud auth application-default login
```

#### Project ID

DataprocMagic tries to infer a Project ID from the selected account. If DataprocMagic does not
find a Project ID, you need to enter one. Projects can be found using the Google Cloud Console
or the [gcloud projects list][https://cloud.google.com/sdk/gcloud/reference/projects/list]
command.

#### Region

Select a region from the Region dropdown.

#### Optional: Cluster and Filter

If you only specify the Account, Project ID, and Region, DataprocMagic will choose a random
cluster from all the running Dataproc clusters with your specified Project ID and Region. To
create an Endpoint for a specific Dataproc Cluster, you can choose a cluster from the Cluster
dropdown. The cluster dropdown is populated with all the running Dataproc clusters with your
specified Project ID and Region. Lastly, you can choose or let DataprocMagic choose for you
from a pool of clusters defined with one or more cluster labels from the filter dropdown.

### %spark magic command

To see all %spark magic subcommands:
```bash
%spark?
```
#### Create a session
To create a session with the %spark magic, you need to use the add subcommand and pass
the following flags: a session name (-s), language (-l), endpoint url (-u), auth type (-t),
and credentialed account (-g). To see all credentialed accounts `gcloud auth list`.
An example, to add a session:
```bash
 `%spark add -s test-session -l python -u https://sparkcluster.net/livy -t Google -g default-credentials
 ```

#### Deleting a session
To delete a session named test-session:
```bash
%spark delete -s defaultlivy
```

#### Listing sessions
To list running sessions:
```bash
%spark info
```

## Installation Troubleshooting

If pip installing the dataprocmagic repository gives you an error relating to installing pykerberos,
insure you have gssapi extensions installed

1. For Debian/Ubuntu/etc:

    ```bash
    sudo apt-get install -y libkrb5-dev
    ```

[cloud_sdk_install]: https://cloud.google.com/sdk/install
[cloud_sdk_initialize]: https://cloud.google.com/sdk/docs/initializing
