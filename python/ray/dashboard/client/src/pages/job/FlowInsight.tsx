import { FlowInsight } from "@ant-ray/flow-insight";
import React from "react";
import { useParams } from "react-router-dom";

type RouteParams = {
  jobId: string;
  [key: string]: string | undefined;
};

const FlowInsightPage = () => {
  const { jobId } = useParams<RouteParams>();

  return <FlowInsight baseUrl="/insight" flowId={jobId} />;
};

export default FlowInsightPage;
