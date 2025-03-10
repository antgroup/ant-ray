export type PhysicalViewData = {
  physicalView: {
    [nodeId: string]: {
      resources: {
        [resourceName: string]: {
          total: number;
          available: number;
        };
      };
      actors: {
        [actorId: string]: {
          actorId: string;
          name: string;
          state: string;
          pid?: number;
          requiredResources?: {
            [resourceName: string]: number;
          };
          placementGroup?: {
            id: string;
          };
        };
      };
    };
  };
};

export const getPhysicalViewData = async (jobId?: string): Promise<PhysicalViewData> => {
  const url = jobId ? `/physical_view?job_id=${jobId}` : '/physical_view';
  const response = await fetch(url);
  
  if (!response.ok) {
    throw new Error('Failed to fetch physical view data');
  }
  
  const result = await response.json();
  
  if (!result.result) {
    throw new Error(result.msg || 'Failed to fetch physical view data');
  }
  
  return result.data;
}; 