import {
  Grid,
  Switch,
  Table,
  TableBody,
  TableCell,
  TableContainer,
  TableHead,
  TableRow,
} from "@material-ui/core";
import Pagination from "@material-ui/lab/Pagination";
import React, { useEffect, useState } from "react";
import { Link } from "react-router-dom";
import PercentageBar from "../../../components/PercentageBar";
import ResourceUnitPie from "../../../components/ResourceUnitPie";
import { getJobResources } from "../../../service/resource";
import { JobInfo } from "../../../type/job";
import { JobResource } from "../../../type/resource";
import { usePage } from "../../../util/hook";

export default (props: { jobId: string; jobInfo: JobInfo }) => {
  const { jobId, jobInfo } = props;
  const [resources, setRes] = useState<JobResource>();
  const [tableMode, setTableMode] = useState(true);
  const jobPage = usePage();
  const nodePage = usePage();

  useEffect(() => {
    getJobResources(jobId).then((res) => {
      setRes(res.data.data.resources);
    });
  }, [jobId]);

  if (!resources || !Object.keys(resources).length) {
    return null;
  }

  const keys = Array.from(
    new Set(
      resources.nodes
        .map((e) => Object.keys(e.jobResources.acquiredResources))
        .flatMap((e) => e),
    ),
  );

  return (
    <div>
      <h4>Total Resources</h4>
      Memory: {jobInfo?.totalMemoryMb || "-"} MB
      <h4>Job Acquired Resource</h4>
      <Pagination
        page={jobPage.page}
        count={Math.ceil((resources.nodes?.length || 0) / jobPage.pageSize)}
        onChange={(e, num) => jobPage.setPage(num)}
      />
      <TableContainer>
        <Table>
          <TableHead>
            <TableRow>
              <TableCell>Node Id</TableCell>
              {keys.map((k) => (
                <TableCell key={k}>{k}</TableCell>
              ))}
            </TableRow>
          </TableHead>
          <TableBody>
            {resources.nodes
              ?.slice(jobPage.startIndex, jobPage.endIndex)
              .map((node) => (
                <TableRow>
                  <TableCell>
                    <Link to={`/node/${node.nodeId}`}>
                      {node.nodeId.slice(0, 5)}
                    </Link>
                  </TableCell>
                  {keys.map((k) => (
                    <TableCell key={k}>
                      {node.jobResources?.acquiredResources[k] || "-"}
                    </TableCell>
                  ))}
                </TableRow>
              ))}
          </TableBody>
        </Table>
      </TableContainer>
      <h4>Node Resource Utilization</h4>
      Chart <Switch
        checked={tableMode}
        onChange={(e, v) => setTableMode(v)}
      />{" "}
      Table
      <Pagination
        page={nodePage.page}
        count={Math.ceil((resources.nodes?.length || 0) / nodePage.pageSize)}
        onChange={(e, num) => nodePage.setPage(num)}
      />
      {tableMode && (
        <Table>
          <TableHead>
            <TableRow>
              {["Node Id", "Memory Used", "CPU Used"].map((col) => (
                <TableCell>{col}</TableCell>
              ))}
            </TableRow>
          </TableHead>
          <TableBody>
            {resources.nodes
              ?.slice(nodePage.startIndex, nodePage.endIndex)
              .map((node) => (
                <TableRow>
                  <TableCell>
                    <Link to={`/node/${node.nodeId}`}>{node.nodeId}</Link>
                  </TableCell>
                  <TableCell>
                    <PercentageBar
                      num={
                        node.nodeResources?.memory?.total -
                        node.nodeResources?.memory?.available
                      }
                      total={node.nodeResources?.memory?.total}
                    >
                      {node.nodeResources?.memory?.available} /{" "}
                      {node.nodeResources?.memory?.total} (
                      {(
                        100 -
                        (node.nodeResources?.memory?.available /
                          node.nodeResources?.memory?.total) *
                          100
                      ).toFixed(0)}
                      %)
                    </PercentageBar>
                  </TableCell>
                  <TableCell>
                    <PercentageBar
                      num={
                        node.nodeResources?.CPU?.total -
                        node.nodeResources?.CPU?.available
                      }
                      total={node.nodeResources?.CPU?.total}
                    >
                      {node.nodeResources?.CPU?.available} /{" "}
                      {node.nodeResources?.CPU?.total} (
                      {(
                        100 -
                        (node.nodeResources?.CPU?.available /
                          node.nodeResources?.CPU?.total) *
                          100
                      ).toFixed(0)}
                      %)
                    </PercentageBar>
                  </TableCell>
                </TableRow>
              ))}
          </TableBody>
        </Table>
      )}
      {!tableMode &&
        resources.nodes
          ?.slice(nodePage.startIndex, nodePage.endIndex)
          .map((node) => (
            <React.Fragment>
              <h5>
                <Link to={`/node/${node.nodeId}`}>{node.nodeId}</Link>
              </h5>
              <Grid container justify="space-around">
                {node.nodeResources?.memory && (
                  <Grid item>
                    <ResourceUnitPie
                      title="Memory"
                      resource={node.nodeResources.memory}
                    />
                  </Grid>
                )}
                {node.nodeResources?.CPU && (
                  <Grid item>
                    <ResourceUnitPie
                      title="CPU"
                      resource={node.nodeResources.CPU}
                    />
                  </Grid>
                )}
              </Grid>
            </React.Fragment>
          ))}
    </div>
  );
};
