import { RayConfigRsp } from "../type/config";
import { get } from "./requestHandlers";
import axios from "axios";
import { ClusterMetrics } from "../hooks/useClusterMetrics";

export const getRayConfig = () => {
  return get<RayConfigRsp>("api/ray_config");
};

export interface ClusterInfo {
  aliveNodes: number;
  deadNodes: number;
  totalCpuUsage: number;
  totalMemoryUsage: number;
  totalGpuUsage?: number;
}

const api = axios.create({
  baseURL: "/api",
});

export const getClusterInfo = () => {
  return api.get<ClusterInfo>("/cluster/info");
};

export const getClusterMetrics = () => {
  return api.get<ClusterMetrics>("/cluster/metrics");
};
