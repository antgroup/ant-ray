from libcpp.string cimport string as c_string
from libcpp cimport bool as c_bool
from libcpp.vector cimport vector as c_vector
from libcpp.memory cimport unique_ptr
from libcpp.pair cimport pair as c_pair
from libcpp.unordered_map cimport unordered_map
from ray.includes.unique_ids cimport (
    CActorID,
    CNodeID,
    CObjectID,
    CWorkerID,
    CPlacementGroupID,
    CJobID,
)
from ray.includes.common cimport (
    CRayStatus,
)

cdef extern from "ray/gcs/gcs_client/global_state_accessor.h" nogil:
    cdef cppclass CGlobalStateAccessor "ray::gcs::GlobalStateAccessor":
        CGlobalStateAccessor(const c_string &redis_address,
                             const c_string &redis_password)
        c_bool Connect()
        void Disconnect()
        c_vector[c_string] GetAllJobInfo()
        c_vector[c_string] GetAllNodeInfo()
        c_vector[c_string] GetAllNodeInfoByNodegroup(
            const c_string &nodegroup_id)
        c_vector[c_string] GetAllAvailableResources()
        c_vector[c_string] GetAllProfileInfo()
        c_vector[c_string] GetAllObjectInfo()
        unique_ptr[c_string] GetObjectInfo(const CObjectID &object_id)
        unique_ptr[c_string] GetAllResourceUsage()
        c_vector[c_string] GetClusterResources()
        c_vector[c_string] GetAllActorInfo()
        unique_ptr[c_string] GetActorInfo(const CActorID &actor_id)
        c_string GetNodeResourceInfo(const CNodeID &node_id)
        unique_ptr[c_string] GetWorkerInfo(const CWorkerID &worker_id)
        c_vector[c_string] GetAllWorkerInfo()
        c_bool AddWorkerInfo(const c_string &serialized_string)
        unique_ptr[c_string] GetPlacementGroupInfo(
            const CPlacementGroupID &placement_group_id)
        unique_ptr[c_string] GetPlacementGroupByName(
            const c_string &placement_group_name,
            const c_string &ray_namespace,
        )
        c_vector[c_string] GetAllPlacementGroupInfo()
        # Below methods are ANT-INTERNAL.
        c_bool UpdateJobResourceRequirements(
            const CJobID &job_id,
            const unordered_map[c_string, double] &min_resource_requirements,
            const unordered_map[c_string, double] &max_resource_requirements
        ) except +
        c_pair[c_bool, c_string] PutJobData(
            const CJobID &job_id, const c_string &key, const c_string &data)
        c_string GetJobData(const CJobID &job_id, const c_string &key)
        c_string GetSystemConfig()
        CRayStatus GetNodeToConnectForDriver(
            const c_string &node_ip_address,
            c_string *node_to_connect)
