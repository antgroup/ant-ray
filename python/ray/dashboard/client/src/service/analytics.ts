import axios from "axios";
import { useQuery } from "react-query";

export interface MetricSample {
  cpuUsage: number;
  taskThroughput: number;
  timestamp: number;
}

export interface AdvancedMetrics {
  samples: MetricSample[];
  averageTaskLatency: number;
  taskSuccessRate: number;
  resourceEfficiency: number;
  insights: string[];
}

const api = axios.create({
  baseURL: "/api",
});

export const getAdvancedMetrics = () => {
  return api.get<AdvancedMetrics>("/analytics/metrics");
};

export const useAdvancedMetrics = () => {
  return useQuery<AdvancedMetrics, Error>(
    "advancedMetrics",
    async () => {
      const response = await getAdvancedMetrics();
      return response.data;
    },
    {
      refetchInterval: 5000, // Refresh every 5 seconds
    }
  );
}; 