import { get } from "./requestHandlers";

export type Breakpoint = {
  actorCls?: string;
  actorName?: string;
  methodName?: string;
  funcName?: string;
  enable: boolean;
  taskId?: string;
};

export type BreakpointResponse = {
  result: boolean;
  msg: string;
  data: {
    breakpoints: Breakpoint[];
  };
};

export type DebugCommandResponse = {
  result: boolean;
  msg: string;
  data: {
    output: string;
  };
};

export type DebugOutputEntry = {
  cmd: string;
  response: string;
  timestamp: number;
};

export type DebugOutputResponse = {
  result: boolean;
  msg: string;
  data: {
    output: string;
  };
};

export const getBreakpoints = async (jobId?: string): Promise<Breakpoint[]> => {
  const path = jobId ? `get_breakpoints?job_id=${jobId}` : "get_breakpoints";
  console.log(`Fetching breakpoints from: ${path}`);
  const result = await get<BreakpointResponse>(path);
  console.log(`Breakpoints response:`, result.data);
  
  // Handle both response formats:
  // 1. Direct array of breakpoints (current server implementation)
  // 2. Structured response with data.breakpoints (expected structure)
  if (result.data && result.data.data && Array.isArray(result.data.data.breakpoints)) {
    return result.data.data.breakpoints;
  }
  
  // If neither format matches, return an empty array
  console.error("Unexpected breakpoints response format:", result.data);
  return [];
};

export const getDebugOutput = async (taskId: string): Promise<string> => {
  if (!taskId) {
    console.error("Missing task ID for getDebugOutput");
    return "";
  }
  
  const path = `get_debug_output?task_id=${taskId}`;
  console.log(`Fetching debug output from: ${path}`);
  
  try {
    const result = await get<DebugOutputResponse>(path);
    console.log(`Debug output response:`, result.data);
    
    if (result.data && result.data.data && result.data.data.output) {
      return result.data.data.output;
    }
    
    // If output is directly on the result.data object
    if (result.data && result.data.data.output) {
      return result.data.data.output;
    }
    
    // If neither format matches, return an empty array
    console.error("Unexpected debug output response format:", result.data);
    return "";
  } catch (error) {
    console.error("Error fetching debug output:", error);
    return "";
  }
};

export const setBreakpoint = async (
  breakpoint: {
    job_id?: string;
    actor_cls?: string;
    actor_name?: string;
    method_name?: string;
    func_name?: string;
    flag?: boolean;
  }
): Promise<boolean> => {
  // Check if we're using the REST endpoint or the new HTTP JSON endpoint
  // The newer implementation uses POST with JSON payload
  try {
    // Build the URL with query parameters for backward compatibility
    let path = "set_breakpoint";
    const queryParams = new URLSearchParams();
    
    if (breakpoint.job_id) {
      queryParams.append("job_id", breakpoint.job_id);
    }
    if (breakpoint.actor_cls) {
      queryParams.append("actor_cls", breakpoint.actor_cls);
    }
    if (breakpoint.actor_name) {
      queryParams.append("actor_name", breakpoint.actor_name);
    }
    if (breakpoint.method_name) {
      queryParams.append("method_name", breakpoint.method_name);
    }
    if (breakpoint.func_name) {
      queryParams.append("func_name", breakpoint.func_name);
    }
    if (breakpoint.flag !== undefined) {
      queryParams.append("flag", breakpoint.flag.toString());
    }
    
    const queryString = queryParams.toString();
    if (queryString) {
      path += `?${queryString}`;
    }
    
    console.log(`Setting breakpoint: ${path}`);
    const result = await get(path);
    console.log(`Set breakpoint response:`, result.data);
    return result.data.result || result.data.success || false;
  } catch (error) {
    console.error("Error setting breakpoint:", error);
    return false;
  }
};

export const sendDebugCommand = async (
  params: {
    job_id?: string;
    task_id?: string;
    cmd: string;
  }
): Promise<string> => {
  let path = "insight_debug?";
  
  if (params.job_id) {
    path += `job_id=${params.job_id}&`;
  }
  if (params.task_id) {
    path += `task_id=${params.task_id}&`;
  }
  path += `cmd=${encodeURIComponent(params.cmd)}`;
  
  console.log(`Sending debug command: ${path}`);
  try {
    const result = await get<DebugCommandResponse>(path);
    console.log(`Debug command response:`, result.data);
    
    // Handle different response formats
    if (result.data && result.data.data && result.data.data.output) {
      return result.data.data.output;
    }
    return "";
  } catch (error) {
    console.error("Error sending debug command:", error);
    return "Error: Command execution failed";
  }
};

export const closeDebugSession = async (jobId: string, taskId: string): Promise<boolean> => {
  if (!taskId) {
    console.error("Missing task ID for closeDebugSession");
    return false;
  }
  
  const path = `close_debug_session?task_id=${taskId}&job_id=${jobId}`;
  console.log(`Closing debug session: ${path}`);
  
  try {
    const result = await get(path);
    console.log(`Close debug session response:`, result.data);
    return result.data.result || result.data.success || false;
  } catch (error) {
    console.error("Error closing debug session:", error);
    return false;
  }
}; 