import axios from "axios";
import { EventGlobalRsp, EventPiplineRsp, EventRsp } from "../type/event";

export const getEvents = (jobId: string) => {
  if (jobId) {
    return axios.get<EventRsp>(`events?job_id=${jobId}`);
  }
};

export const getPipelineEvents = (jobId: string) => {
  if (jobId) {
    return axios.get<EventRsp>(`events?job_id=${jobId}&view=pipeline`);
  }
};
export const getActorPipelineEvents = (actorId: string) => {
  if (actorId) {
    return axios.get<EventPiplineRsp>(
      `events/pipeline?query_id=${actorId}&view=actor`,
    );
  }
};

export const getWorkerPipelineEvents = (workerId: string, type: string) => {
  if (workerId) {
    return axios.get<EventPiplineRsp>(
      `events/pipeline?query_id=${workerId}&view=worker&query_type=${type}`,
    );
  }
};
export const getGlobalEvents = () => {
  return axios.get<EventGlobalRsp>("events");
};
