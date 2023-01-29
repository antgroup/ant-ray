import { Button } from "@material-ui/core";
import React, { useCallback, useEffect, useState } from "react";
import { getJobResult } from "../service/job";
import LogVirtualView from "./LogView/LogVirtualView";

const JobResult = ({ jobId }: { jobId: string }) => {
  const [result, setResult] = useState<string>("");
  const getResult = useCallback(async () => {
    const { data } = await getJobResult(jobId);
    setResult(JSON.stringify(data.data.result, null, 4));
  }, [jobId]);
  useEffect(() => {
    getResult();
  }, [getResult]);

  return (
    <div>
      <Button onClick={getResult}>Refresh</Button>
      <LogVirtualView content={result} language="json" />
    </div>
  );
};

export default JobResult;
