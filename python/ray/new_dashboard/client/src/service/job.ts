import axios from "axios";
import { JobDetailRsp, JobListRsp, JobResultRsp } from "../type/job";

export const getJobList = (includeInactive?: boolean) => {
  const url = `jobs?view=summary${includeInactive ? '' : '&exclude-inactive=true'}`

  return axios.get<JobListRsp>(url);
};

export const getJobDetail = (id: string) => {
  return axios.get<JobDetailRsp>(`jobs/${id}`);
};

export const getJobResult = (id: string) => {
  return axios.get<JobResultRsp>(`jobs/${id}?view=result`);
};
