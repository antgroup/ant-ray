Virtual Cluster Management API
==============================

.. _virtual-cluster-management-api:

This page introduces the management API of the Ray virtual clusters:

.. contents::
    :local:

.. _virtual-cluster-create-or-update-virtual-cluster:

Create or Update A Virtual Cluster
----------------------------------

To create or update a virtual cluster, users can send a POST request to the http endpoint at `/virtual_clusters`.

**Sample Request:**

.. code-block:: json

    {
        "virtualClusterId":"virtual_cluster_1", // Unique id of the virtual cluster
        "divisible":false,  // Whether it is a divisible virtual cluster
        "replicaSets": {    // The node type (same as pod template id) and count that you expect to assign to this virtual cluster
            "4c8g":1,
            "8c16g":1
        },
        "revision":1734141542694321600  // The timestamp of the virtual cluster's most recent creation/update
    }

**Success Response:**

.. code-block:: json

    {
        "result":true,
        "msg":"Virtual cluster created or updated.",
        "data":{
            "virtualClusterId":"virtual_cluster_1",
            "revision":1734141542694433731, // The timestamp that this creation/update request was enforced in gcs
            "nodeInstances": {   // The nodes that were actually assigned to this virtual cluster
                "033141204224b43e67f01ec314ba45c16892298a23e83c5182eec355":{    // The node id used in gcs
                    "hostname": "ec2-33-141-204-224.us-west-2.compute.amazonaws.com",
                    "templateId": "4c8g"
                },
                "033159116236f3f382597f5e05cadbc000655f862f389c41072cef73": {
                    "hostname": "ec2-33-159-116-236.us-west-2.compute.amazonaws.com",
                    "templateId": "8c16g"
                }
            }
        }
    }

**Error Response:**

- If there are not enough eligible nodes to be added or removed, the sample reply will be:

.. code-block:: json

    {
        "result":false,
        "msg":"Failed to create or update virtual cluster virtual_cluster_1: No enough nodes to add to the virtual cluster. The replica sets that gcs can add at most are shown below. Use it as a suggestion to adjust your request or cluster.",
        "data":{
            "virtualClusterId":"virtual_cluster_1",
            "replicaSetsToRecommend":{
                "4c8g":1
            }
        }
    }

The reply will tell you the replica sets that are allowed to added or removed at most. You can use this information to adjust your request.

- If you update the virtual cluster with an expired revision, then the sample reply will be:

.. code-block:: json

    {
        "result":false,
        "msg":"Failed to create or update virtual cluster virtual_cluster_1: The revision (0) is expired, the latest revision of the virtual cluster virtual_cluster_1 is 1736848949051567437",
        "data":{
            "virtualClusterId":"virtual_cluster_1",
            "replicaSetsToRecommend":{}
        }
    }

Every time you want to update a virtual cluster, do it based on the latest revision (there might be more than one party of interest). The failure message in the reply above tells you the latest revision. You can also get it by accessing the GET API shown below.

Remove A Virtual Cluster
------------------------

Users can remove a virtual cluster by sending a DELETE request to the http endpoint at `/virtual_clusters/{virtual_cluster_id}`.

**Success Response:**

.. code-block:: json

    {
        "result":true,
        "msg":"Virtual cluster virtual_cluster_1 removed.",
        "data":{
            "virtualClusterId":"virtual_cluster_1"
        }
    }

**Error Response:**

If there are still jobs running in the virtual cluster, then the sample reply will be:

.. code-block:: json

    {
        "result":false,
        "msg":"Failed to remove virtual cluster virtual_cluster_1: The virtual cluster virtual_cluster_1 can not be removed as it is still in use. ",
        "data":{
            "virtualClusterId":"virtual_cluster_1"
        }
    }

Remove Nodes From A Virtual Cluster
-----------------------------------

Users can remove some specified nodes from a virtual cluster by sending a POST request to the http endpoint at `/virtual_clusters/remove_nodes`.

After nodes are removed from a virtual cluster, they are returned to the primary cluster.

Currently, we do not support removing specified nodes from a job cluster.

**Sample Request:**

.. code-block:: json

    {
        "virtualClusterId":"virtual_cluster_1", // Unique id of the virtual cluster
        "nodesToRemove":[  // The list of node id that are expected to be removed
            "1434167efc236b03e1618ba59b5210dd4a7399287389606792eac8cf",
            "805faffa6c48e77407ef7e2e62b30d0af914e4a92837468987ad8dbe"
        ],
    }

**Success Response:**

.. code-block:: json

    {
        "result":true,
        "msg":"Virtual cluster successfully updated.",
        "data":{
            "virtualClusterId":"virtual_cluster_1"
        }
    }

**Error Response:**

If there are any nodes that are not allowed for removal, the request would fail and the sample reply will be:

.. code-block:: json

    {
        "result":false,
        "msg":"Failed to remove nodes from virtual cluster virtual_cluster_1: Failed to remove some of the nodes because they are not idle nor found in the virtual cluster. These nodes with failure are shown below.",
        "data":{
            "virtualClusterId":"virtual_cluster_1",
            "nodesWithFailure":[
                "805faffa6c48e77407ef7e2e62b30d0af914e4a92837468987ad8dbe"
            ]
        }
    }

Get Virtual Clusters
--------------------

To get the metadata of all virtual clusters, users can send a GET request to the http endpoint at `/virtual_clusters`.

**Success Response:**

.. code-block:: json

    {
        "result":true,
        "msg":"All virtual clusters fetched.",
        "data":{
            "virtualClusters":[
                {
                    "virtualClusterId":"virtual_cluster_1",
                    "divisible":false,
                    "isRemoved":false,
                    "nodeInstances":{  // The nodes assigned to this virtual cluster.
                    "033141204224b43e67f01ec314ba45c16892298a23e83c5182eec355":{
                        "hostname":"ec2-33-141-204-224.us-west-2.compute.amazonaws.com",
                        "templateId":"4c8g"
                    },
                    "033159116236f3f382597f5e05cadbc000655f862f389c41072cef73":{
                        "hostname":"ec2-33-159-116-236.us-west-2.compute.amazonaws.com",
                        "templateId":"8c16g"
                    }
                    },
                    "revision":1734141542694433731  // The timestamp of the virtual cluster's most recent creation/update.
                },
                {
                    "virtualClusterId":"virtual_cluster_2",
                    "divisible":true,
                    "isRemoved":false,
                    "nodeInstances":{
                    "0331761541565ea3c14fcc158a98e9a6eed9e0c3c6c86fa613ce6738":{
                        "hostname":"ec2-33-176-154-156.us-west-2.compute.amazonaws.com",
                        "templateId":"8c16g"
                    },
                    "0331280722461e5130088465a89bd8262738fbd301ae9ae06e1edf42":{
                        "hostname":"ec2-33-128-72-246.us-west-2.compute.amazonaws.com",
                        "templateId":"4c8g"
                    }
                    },
                    "revision":1734132897622670263
                }
            ]
        }
    }

Update Autoscaling Config
-------------------------

Users can update the autoscaling config (e.g., min/max limits) of a virtual cluster by sending a POST request to the http endpoint at `/virtual_clusters/update_autoscaling_config`.

Once the autoscaling config is updated, the autoscaler (v2 only) will try to enforce it.

**Sample Request:**

.. code-block:: json

    {
        "virtualClusterId":"virtual_cluster_1",
        "minReplicaSets":{ // The min replica sets of each node type.
            "4c8g":1,
            "8c16g":0
        },
        "maxReplicaSets":{ // The max replica sets of each node type.
            "4c8g":2,
            "8c16g":2
        },
        "maxNodes":4 // The max total node count
    }

**Sample Response:**

.. code-block:: json

    {
        "result":true,
        "msg":"Virtual cluster virtual_cluster_1's autoscaling config updated.",
        "data":{
            "virtualClusterId":"virtual_cluster_1"
        }
    }

Get Autoscaling Config
----------------------

To get the autoscaling config of a virtual clusters, users can send a GET request to the http endpoint at `/virtual_clusters/autoscaling_config/{virtual_cluster_id}`.

**Success Response:**

.. code-block:: json

    {
        "result":true,
        "msg":"Autoscaling config fetched.",
        "data":{
            "minReplicaSets":{
                "4c8g":1,
                "8c16g":0
            },
            "maxReplicaSets":{
                "4c8g":2,
                "8c16g":2
            },
            "maxNodes":4
        }
    }
