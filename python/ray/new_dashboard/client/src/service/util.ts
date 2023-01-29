import axios from "axios";

type CMDRsp = {
  result: boolean;
  msg: string;
  data: {
    output: string;
  };
};

export const getJstack = (ip: string, pid: string) => {
  return axios.get<CMDRsp>("utils/jstack", {
    params: {
      ip,
      pid,
    },
  });
};

export const getJmap = (ip: string, pid: string) => {
  return axios.get<CMDRsp>("utils/jmap", {
    params: {
      ip,
      pid,
    },
  });
};

export const getJstat = (ip: string, pid: string, options: string) => {
  return axios.get<CMDRsp>("utils/jstat", {
    params: {
      ip,
      pid,
      options,
    },
  });
};

export const getPystack = (ip: string, pid: string) => {
  return axios.get<CMDRsp>("utils/pystack", {
    params: {
      ip,
      pid,
    },
  });
};

export const getPstack = (ip: string, pid: string) => {
  return axios.get<CMDRsp>("utils/pstack", {
    params: {
      ip,
      pid,
    },
  });
};

type NamespacesRsp = {
  result: boolean;
  msg: string;
  data: {
    namespaces: {
      namespaceId: string;
      hostNameList: string[];
    }[];
  };
};

export const getNamespaces = () => {
  return axios.get<NamespacesRsp>("namespaces");
};

type FileGraphRsp = {
  result: boolean;
  msg: string;
  data: {
    output: string;
  };
};

export const getFireGraph = (
  ip: string,
  duration: number,
  pid: number,
  profiler = "async-profiler",
) => {
  return axios.get<FileGraphRsp>(`utils/${profiler}`, {
    params: {
      ip,
      pid,
      duration,
    },
  });
};
