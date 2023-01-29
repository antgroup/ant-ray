import axios from "axios";
import {
  ClusterResourcesRsp,
  JobResourceRsp,
  NamespaceLayerdResourceRsp,
  NamespaceResourceRsp,
  NodeResourceRsp,
} from "../type/resource";

export const getClusterResource = async () => {
  return await axios.get<ClusterResourcesRsp>("resource/cluster");
};

export const getJobResources = (jobId: string) => {
  return axios.get<JobResourceRsp>(`resource/job/${jobId}`);
};

export const getNodeResources = (nodeId: string) => {
  return axios.get<NodeResourceRsp>(`resource/node/${nodeId}`);
};

export const getNamespacesResource = () => {
  return axios.get<NamespaceResourceRsp>("namespaces/resources");
};

export const getNamespacesLayeredResource = () => {
  return axios.get<NamespaceLayerdResourceRsp>("namespaces/layered_resources");
};
