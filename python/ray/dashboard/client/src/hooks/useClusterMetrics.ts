import { useQuery } from "react-query";
import { getClusterMetrics } from "../service/cluster";

export interface ClusterMetrics {
  timestamps: number[];
  cpuUsage: number[];
  memoryUsage: number[];
  gpuUsage?: number[];
}

export const useClusterMetrics = () => {
  return useQuery<ClusterMetrics, Error>(
    "clusterMetrics",
    async () => {
      const response = await getClusterMetrics();
      return response.data;
    },
    {
      refetchInterval: 5000, // Refresh every 5 seconds
    }
  );
}; 