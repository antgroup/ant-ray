import {
  JobListRsp,
  StateApiJobProgressByTaskNameRsp,
  StateApiNestedJobProgressRsp,
  UnifiedJob,
} from "../type/job";
import { get } from "./requestHandlers";
import axios from "axios";
import { useQuery } from "react-query";

export interface DurationHistogramBin {
  start: number;
  end: number;
  count: number;
}

export interface JobStats {
  total: number;
  pending: number;
  running: number;
  succeeded: number;
  failed: number;
  averageDuration: number;
  durationHistogram: DurationHistogramBin[];
}

const api = axios.create({
  baseURL: "/api",
});

export const getJobList = () => {
  return get<JobListRsp>("api/jobs/");
};

export const getJobDetail = (id: string) => {
  return get<UnifiedJob>(`api/jobs/${id}`);
};

export const getStateApiJobProgressByTaskName = (jobId: string) => {
  return get<StateApiJobProgressByTaskNameRsp>(
    `api/v0/tasks/summarize?filter_keys=job_id&filter_predicates=%3D&filter_values=${jobId}`,
  );
};

export const getStateApiJobProgressByLineage = (jobId: string) => {
  return get<StateApiNestedJobProgressRsp>(
    `api/v0/tasks/summarize?filter_keys=job_id&filter_predicates=%3D&filter_values=${jobId}&summary_by=lineage`,
  );
};

export const getJobStats = () => {
  return api.get<JobStats>("/jobs/stats");
};

export const useJobStats = () => {
  return useQuery<JobStats, Error>(
    "jobStats",
    async () => {
      const response = await getJobStats();
      return response.data;
    },
    {
      refetchInterval: 5000, // Refresh every 5 seconds
    }
  );
};
