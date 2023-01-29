import { Grid, Tooltip } from "@material-ui/core";
import {
  AccessTimeOutlined,
  ArrowRightOutlined,
  CancelOutlined,
  CheckCircleOutline,
} from "@material-ui/icons";
import dayjs from "dayjs";
import React, { useEffect, useRef, useState } from "react";
import { getPipelineEvents } from "../service/event";

const timeFormat = (time: number, len: number = 1) => {
  const ms = time % 1000;
  const s = Math.floor(time / 1000);
  const m = Math.floor(s / 60);
  const h = Math.floor(m / 60);
  const d = Math.floor(h / 24);
  const rst = [];

  if (ms > 0) {
    rst.push(`${ms}ms`);
  }

  if (s % 60 > 0) {
    rst.push(`${s % 60}s`);
  }

  if (m % 60 > 0) {
    rst.push(`${m % 60}min`);
  }

  if (h % 24 > 0) {
    rst.push(`${h % 24}h`);
  }

  if (d > 0) {
    rst.push(`${d}day`);
  }

  return rst.slice(-len).reverse().join("");
};

enum PIPELINE_STATUS {
  WAITING,
  ERROR,
  SUCCESS,
}

const { WAITING, ERROR, SUCCESS } = PIPELINE_STATUS;

const createPipeline = (label: string[], name: string) => ({
  label,
  state: WAITING,
  name,
  msg: "Not Processed Yet",
  time: "not yet",
  timestamp: 0,
});

const DefaultPipeline = [
  ["Job Submmited", "JOB_PIPELINE_SUBMMITED"],
  ["Resources Check", "JOB_PIPELINE_RESOURCES_CHECK"],
  ["Job Added", "JOB_PIPELINE_ADDED"],
  ["Env Prepare Start", "JOB_PIPELINE_ENV_PREPARE_START"],
  ["Env Prepare End", "JOB_PIPELINE_ENV_PREPARE_END"],
  ["Driver Start", "JOB_PIPELINE_DRIVER_START"],
  ["Driver Exit", "JOB_PIPELINE_DRIVER_EXIT"],
  [
    "Job Complete",
    "JOB_PIPELINE_COMPLETE",
    "JOB_PIPELINE_LONG_RUNNING",
    "JOB_PIPELINE_FINISHED",
  ],
].map(([a, ...c]) => createPipeline(c, a));

const useJobPipeline = (jobID: string) => {
  const [pipelines, setPipe] = useState([
    {
      name: "Loading",
      time: "not yet",
      state: WAITING,
      msg: "Loading",
      timestamp: 0,
    },
  ]);

  const tmot = useRef<ReturnType<typeof setTimeout>>();

  useEffect(() => {
    const getPipe = () =>
      getPipelineEvents(jobID)?.then((res) => {
        if (res?.data?.data?.events) {
          const eventpool = res.data.data.events;
          setPipe(
            DefaultPipeline.map((pipe) => {
              const target = eventpool.find((e) =>
                pipe.label.includes(e.label),
              );

              const newPipe = { ...pipe };

              if (!target) {
                return newPipe;
              }

              if (target.severity === "INFO") {
                newPipe.state = SUCCESS;
                if (target.label === "JOB_PIPELINE_LONG_RUNNING") {
                  newPipe.name = "Job Long Running";
                } else if (target.label === "JOB_PIPELINE_FINISHED") {
                  newPipe.name = "Job Finished";
                }
              } else if (target.severity === "ERROR") {
                newPipe.state = ERROR;
              }

              newPipe.msg = target.message;
              if (target.timeStamp) {
                target.timestamp = dayjs(target.timeStamp).valueOf();
              } else {
                target.timestamp = target.timestamp * 1000;
              }
              newPipe.time = dayjs(Math.round(Number(target.timestamp))).format(
                "MM/DD HH:mm:ss",
              );
              newPipe.timestamp = target.timestamp;
              return newPipe;
            }),
          );
        }
        tmot.current = setTimeout(getPipe, 4000);
      });

    getPipe();
    return () => tmot.current && clearTimeout(tmot.current);
  }, [jobID]);

  return {
    pipelines: pipelines.filter(
      (e, i) => e.state !== WAITING || pipelines[i + 1]?.state === WAITING,
    ),
  };
};

const IconMap = {
  [WAITING]: <AccessTimeOutlined color="disabled" fontSize="small" />,
  [SUCCESS]: <CheckCircleOutline color="secondary" fontSize="small" />,
  [ERROR]: <CancelOutlined color="error" fontSize="small" />,
};

const JobPipeline = ({ job_id }: { job_id: string }) => {
  const { pipelines } = useJobPipeline(job_id);

  return (
    <Grid container spacing={6} justify="space-evenly" alignItems="center">
      {pipelines
        .filter(
          (e, i) =>
            !(
              e.state === WAITING &&
              pipelines[i + 1] &&
              pipelines[i + 1].state !== WAITING
            ),
        )
        .map((e, i) => (
          <React.Fragment>
            <Grid
              item
              style={{ fontSize: 12, textAlign: "center", padding: "24px 0" }}
            >
              <Tooltip
                arrow
                interactive
                placement="top"
                title={
                  <div>
                    <pre>{e.msg}</pre>
                    <p>{e.time}</p>
                  </div>
                }
              >
                <div style={{ display: "flex", alignItems: "center" }}>
                  {IconMap[e.state]}{" "}
                  <span style={{ marginLeft: 6 }}>{e.name}</span>
                </div>
              </Tooltip>
            </Grid>
            {i < pipelines.length - 1 && (
              <Grid style={{ textAlign: "center" }}>
                <Tooltip
                  arrow
                  interactive
                  placement="top"
                  title={timeFormat(
                    Math.floor(pipelines[i + 1].timestamp - e.timestamp),
                    3,
                  )}
                >
                  <div>
                    <ArrowRightOutlined />
                  </div>
                </Tooltip>
              </Grid>
            )}
          </React.Fragment>
        ))}
    </Grid>
  );
};

export default JobPipeline;
