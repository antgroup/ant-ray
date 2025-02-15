import { NodeDetailRsp, NodeListRsp } from "../type/node";
import { get } from "./requestHandlers";
import axios from "axios";
import { useQuery } from "react-query";

export const getNodeList = async () => {
  return await get<NodeListRsp>("nodes?view=summary");
};

export const getNodeDetail = async (id: string) => {
  return await get<NodeDetailRsp>(`nodes/${id}`);
};

export interface NodeMetrics {
  timestamps: number[];
  cpuUsage: number[];
  memoryUsage: number[];
  networkReceived: number[];
  networkSent: number[];
  totalNodes: number;
  activeNodes: number;
  totalCpuCores: number;
  totalMemoryGB: number;
}

const api = axios.create({
  baseURL: "/api",
});

export const getNodeMetrics = () => {
  return api.get<NodeMetrics>("/nodes/metrics");
};

export const useNodeMetrics = () => {
  return useQuery<NodeMetrics, Error>(
    "nodeMetrics",
    async () => {
      const response = await getNodeMetrics();
      return response.data;
    },
    {
      refetchInterval: 5000, // Refresh every 5 seconds
    }
  );
};
