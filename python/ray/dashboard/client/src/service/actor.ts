import { ActorDetail } from "../type/actor";
import { get } from "./requestHandlers";
import axios from "axios";
import { useQuery } from "react-query";

export const getActors = () => {
  return get<{
    result: boolean;
    message: string;
    data: {
      actors: {
        [actorId: string]: ActorDetail;
      };
    };
  }>("logical/actors");
};

export type ActorResp = {
  result: boolean;
  msg: string;
  data: {
    detail: ActorDetail;
  };
};

export const getActor = (actorId: string) => {
  return get<ActorResp>(`logical/actors/${actorId}`);
};

export interface ActorGroup {
  name: string;
  cpuUsage: number;
  memoryUsage: number;
  numActors: number;
}

export interface ActorGroupsSummary {
  alive: number;
  dead: number;
  restarting: number;
}

export interface ActorGroupsResponse {
  summary: ActorGroupsSummary;
  groups: ActorGroup[];
}

const api = axios.create({
  baseURL: "/api",
});

export const getActorGroups = () => {
  return api.get<ActorGroupsResponse>("/actors/groups");
};

// Custom hook for fetching actor groups
export const useActorGroups = () => {
  return useQuery<ActorGroupsResponse, Error>(
    "actorGroups",
    async () => {
      const response = await getActorGroups();
      return response.data;
    },
    {
      refetchInterval: 5000, // Refresh every 5 seconds
    }
  );
};
