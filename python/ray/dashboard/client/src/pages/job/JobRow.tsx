import { Box, Link, TableCell, TableRow, Tooltip } from "@mui/material";
import React from "react";
import { Link as RouterLink } from "react-router-dom";
import { CodeDialogButtonWithPreview } from "../../common/CodeDialogButton";
import { DurationText } from "../../common/DurationText";
import { formatDateFromTimeMs } from "../../common/formatUtils";
import { JobStatusWithIcon } from "../../common/JobStatus";
import {
  CpuProfilingLink,
  CpuStackTraceLink,
  MemoryProfilingButton,
} from "../../common/ProfilingLink";
import { UnifiedJob } from "../../type/job";
import { useJobProgress } from "./hook/useJobProgress";
import { MiniTaskProgressBar } from "./TaskProgressBar";

type JobRowProps = {
  job: UnifiedJob;
};

export const JobRow = ({ job }: JobRowProps) => {
  const {
    job_id,
    submission_id,
    driver_info,
    status,
    message,
    start_time,
    end_time,
    entrypoint,
  } = job;
  const { progress, error, driverExists } = useJobProgress(job_id ?? undefined);

  const progressBar = (() => {
    if (!driverExists) {
      return <MiniTaskProgressBar />;
    }
    if (!progress || error) {
      return "unavailable";
    }
    if (status === "SUCCEEDED" || status === "FAILED") {
      // TODO(aguo): Show failed tasks in progress bar once supported.
      return <MiniTaskProgressBar {...progress} showAsComplete />;
    } else {
      return <MiniTaskProgressBar {...progress} />;
    }
  })();

  const jobId = job_id ? job_id : submission_id;

  return (
    <TableRow>
      <TableCell align="center">
        {job_id ? (
          <Link component={RouterLink} to={job_id}>
            {job_id}
          </Link>
        ) : submission_id ? (
          <Link component={RouterLink} to={submission_id}>
            (no ray driver)
          </Link>
        ) : (
          "(no ray driver)"
        )}
      </TableCell>
      <TableCell align="center">{submission_id ?? "-"}</TableCell>
      <TableCell align="center">
        <Tooltip title={entrypoint} arrow>
          <Box
            sx={{
              display: "block",
              margin: "auto",
              maxWidth: 360,
              textOverflow: "ellipsis",
              overflow: "hidden",
              whiteSpace: "nowrap",
            }}
          >
            {entrypoint}
          </Box>
        </Tooltip>
      </TableCell>
      <TableCell align="center">
        <JobStatusWithIcon job={job} />
      </TableCell>
      <TableCell align="center">
        {message ? (
          <CodeDialogButtonWithPreview
            sx={{
              maxWidth: 250,
              display: "inline-flex",
            }}
            title="Status message"
            code={message}
          />
        ) : (
          "-"
        )}
      </TableCell>
      <TableCell align="center">
        {start_time && start_time > 0 ? (
          <DurationText startTime={start_time} endTime={end_time} />
        ) : (
          "-"
        )}
      </TableCell>
      <TableCell align="center">{progressBar}</TableCell>
      <TableCell align="center">
        {jobId && (
          <React.Fragment>
            <Link component={RouterLink} to={jobId}>
              Log
            </Link>
            <br />
            <Link component={RouterLink} to={`${jobId}/graph`}>
              Insight
            </Link>
            <br />
          </React.Fragment>
        )}
        <CpuStackTraceLink
          pid={job.driver_info?.pid}
          ip={job.driver_info?.node_ip_address}
          type="Driver"
        />
        <br />
        <CpuProfilingLink
          pid={job.driver_info?.pid}
          ip={job.driver_info?.node_ip_address}
          type="Driver"
        />
        <br />
        <MemoryProfilingButton
          pid={job.driver_info?.pid}
          ip={job.driver_info?.node_ip_address}
          type="Driver"
        />
      </TableCell>
      <TableCell align="center">
        {start_time ? formatDateFromTimeMs(start_time) : "-"}
      </TableCell>
      <TableCell align="center">
        {end_time && end_time > 0 ? formatDateFromTimeMs(end_time) : "-"}
      </TableCell>
      <TableCell align="center">{driver_info?.pid ?? "-"}</TableCell>
    </TableRow>
  );
};
