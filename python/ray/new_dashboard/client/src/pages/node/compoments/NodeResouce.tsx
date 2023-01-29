import {
  Grid,
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
import ResourceUnitPie from "../../../components/ResourceUnitPie";
import { SearchSelect } from "../../../components/SearchComponent";
import { getNodeResources } from "../../../service/resource";
import { NodeResource } from "../../../type/resource";
import { usePage, useSorter } from "../../../util/hook";

export default (props: { nodeId: string }) => {
  const { nodeId } = props;
  const [resources, setRes] = useState<NodeResource>();
  const { page, startIndex, endIndex, setPage, pageSize } = usePage();
  const { setSortKey, sorterFunc } = useSorter();

  useEffect(() => {
    getNodeResources(nodeId).then((res) => {
      if (res.data.data.resources) {
        setRes(res.data.data.resources);
      }
    });
  }, [nodeId]);

  if (!resources || !Object.keys(resources).length) {
    return null;
  }

  const keys = Array.from(
    new Set(
      resources.jobs
        .map((e) => Object.keys(e.jobResources.acquiredResources))
        .flatMap((e) => e),
    ),
  );

  const totalPage = Math.ceil((resources.jobs?.length || 0) / pageSize);

  return (
    <div>
      <h4>Resource Utilization</h4>
      <Grid container justify="space-around">
        {resources?.resources?.memory && (
          <Grid item>
            <ResourceUnitPie
              title="Memory"
              resource={resources.resources.memory}
            />
          </Grid>
        )}
        {resources?.resources?.CPU && (
          <Grid item>
            <ResourceUnitPie title="CPU" resource={resources.resources.CPU} />
          </Grid>
        )}
      </Grid>
      <h4>Job Acquired Resource</h4>
      <Pagination
        page={page}
        onChange={(e, num) => setPage(num)}
        count={totalPage}
      />
      <SearchSelect
        label="Sorter"
        options={["", ...keys]}
        onChange={(val) => setSortKey(val)}
      />
      <TableContainer>
        <Table>
          <TableHead>
            <TableRow>
              <TableCell>Job Id</TableCell>
              {keys.map((k) => (
                <TableCell key={k}>{k}</TableCell>
              ))}
            </TableRow>
          </TableHead>
          <TableBody>
            {resources.jobs
              ?.sort(sorterFunc)
              .slice(startIndex, endIndex)
              .map((job) => (
                <TableRow>
                  <TableCell>
                    <Link to={`/job/${job.jobId}`}>{job.jobId}</Link>
                  </TableCell>
                  {keys.map((k) => (
                    <TableCell key={k}>
                      {job.jobResources?.acquiredResources[k] || "-"}
                    </TableCell>
                  ))}
                </TableRow>
              ))}
          </TableBody>
        </Table>
      </TableContainer>
    </div>
  );
};
