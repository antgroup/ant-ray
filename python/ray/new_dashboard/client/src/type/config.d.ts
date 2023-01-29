export type RayConfig = {
  userName: string;
  workNodeNumber: number;
  headNodeNumber: number;
  containerVcores: number;
  containerMemory: number;
  clusterName: string;
  supremeFo: boolean;
  jobManagerPort: number;
  externalRedisAddresses: string;
  envParams: string;
  sourceCodeLink: string;
  imageUrl: string;
  slsUrl:
    | {
        [key: string]: string;
      }
    | string;
  metricUrl: string;
  rayOperatorEventUrl: string;
  eventUrl: string;
};

export type RayConfigRsp = {
  result: boolean;
  msg: string;
  data: {
    config: RayConfig;
  };
};
