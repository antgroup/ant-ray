export type ResourceUnit = {
  total: number;
  available: number;
  resourceName?: string;
};

export type ClusterResources = {
  [key: string]: ResourceUnit;
};

export type ClusterResourcesRsp = {
  result: boolean;
  msg: string;
  data: {
    timestamp: string;
    resources: ClusterResources;
  };
};

type ACResourceUnit = {
  pid?: string;
  acquiredResources: {
    [key: string]: number;
  };
};

type JobNodeResource = {
  nodeId: string;
  jobResources: {
    acquiredResources: {
      [key: string]: number;
    };
  };
  processResources: ACResourceUnit[];
  nodeResources: ClusterResources;
};

type JobResource = {
  jobId: string;
  acquiredResources: {
    [key: string]: number;
  };
  nodes: JobNodeResource[];
};

export type JobResourceRsp = {
  result: boolean;
  msg: string;
  data: {
    resources: JobResource;
  };
};

type NodeJobResource = {
  jobId: string;
  jobResources: ACResourceUnit;
  processResources: ACResourceUnit[];
};

type NodeResource = {
  nodeId: string;
  resources: {
    [key: string]: ResourceUnit;
  };
  jobs: NodeJobResource[];
};

export type NodeResourceRsp = {
  result: boolean;
  msg: string;
  data: {
    resources: NodeResource;
  };
};

export type HostResource = {
  hostName: string;
  resources: ResourceUnit[];
};

export type ResourceView = {
  namespaceId: string;
  hostResources: HostResource[];
};

export type NamespaceResourceRsp = {
  result: boolean;
  msg: string;
  data: {
    resourceView: ResourceView[];
  };
};

export type HostResourceView = {
  hostName: string;
  resources: ResourceUnit[];
};

export type NamespaceResourceView = {
  namespaceId: string;
  resources: ResourceUnit[];
  unassignedHosts: {
    resources: ResourceUnit[];
    hosts: HostResourceView[];
  };
  namespaces: NamespaceResourceView[];
  hosts: HostResourceView[];
};

export type NamespaceLayerdResourceRsp = {
  result: boolean;
  msg: string;
  data: {
    resourceView: NamespaceResourceView;
  };
};
