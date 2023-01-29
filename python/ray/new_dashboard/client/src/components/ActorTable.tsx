import {
  Button,
  Chip,
  Grid,
  InputAdornment,
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableRow,
  TextField,
  TextFieldProps,
  Tooltip,
} from "@material-ui/core";
import { orange } from "@material-ui/core/colors";
import { ArrowRight, SearchOutlined } from "@material-ui/icons";
import Autocomplete from "@material-ui/lab/Autocomplete";
import Pagination from "@material-ui/lab/Pagination";
import dayjs from "dayjs";
import React, { useContext, useEffect, useMemo, useState } from "react";
import { Link } from "react-router-dom";
import { GlobalContext } from "../App";
import { getActorPipelineEvents } from "../service/event";
import { Actor } from "../type/actor";
import { EventPiplineObject } from "../type/event";
import { Worker } from "../type/worker";
import { useFilter, usePage } from "../util/hook";
import CopyableCollapse from "./CopyableCollapse";
import StateCounter from "./StatesCounter";
import { StatusChip } from "./StatusChip";
import RayletWorkerTable, { ExpandableTableRow } from "./WorkerTable";

const ActorPipeline = ({ actorId }: { actorId: string }) => {
  const [eventList, setEventList] = useState<{
    [key: number]: EventPiplineObject;
  }>({});
  const { ipLogMap, nodeIpMap } = useContext(GlobalContext);
  const [slsUrl, setSlsUrl] = useState<string>();
  const { page, pageSize, setPage, setTotal, total, startIndex, endIndex } =
    usePage();

  useEffect(() => {
    getActorPipelineEvents(actorId)?.then((res) => {
      if (res?.data?.data?.events) {
        setEventList(res.data.data.events);
      }
      setSlsUrl(res.data.data.slsUrl);
    });
  }, [actorId]);

  useEffect(() => {
    setTotal(Object.values(eventList).length);
    // eslint-disable-next-line
  }, [eventList]);

  return (
    <Grid container justify="space-around" alignItems="center">
      {slsUrl && (
        <Button>
          <a href={slsUrl} target="_blank" rel="noopener noreferrer">
            History sls worker log
          </a>
        </Button>
      )}
      {total > pageSize && (
        <Pagination
          page={page}
          count={Math.round(total / pageSize)}
          onChange={(e, p) => setPage(p)}
        />
      )}

      <Table>
        <TableHead>
          {[
            "",
            "Host Name",
            "Job Name",
            "Job Id",
            "Language",
            "Node",
            "Pid",
            "WorkerId",
            "Log",
          ].map((col) => (
            <TableCell align="center" key={col}>
              {col}
            </TableCell>
          ))}
        </TableHead>
        <TableBody>
          {Object.values(eventList)
            .slice(startIndex, endIndex)
            .map((e, i) => (
              <ExpandableTableRow
                length={1}
                expandComponent={
                  <div>
                    {e.info?.message && (
                      <pre style={{ whiteSpace: "pre-wrap" }}>
                        Actor Message: {e.info.message}
                      </pre>
                    )}
                    <Grid container alignItems="center" justify="space-around">
                      {e.events
                        .sort(
                          (a, b) =>
                            dayjs(a.timeStamp).valueOf() -
                            dayjs(b.timeStamp).valueOf(),
                        )
                        .map(({ label, timeStamp, severity, message }, i) => (
                          <React.Fragment>
                            <Grid item style={{ textAlign: "center" }}>
                              <Tooltip title={message} interactive>
                                <p>
                                  {
                                    <StatusChip
                                      status={label}
                                      type={severity}
                                    />
                                  }
                                </p>
                              </Tooltip>
                              <p>{dayjs(timeStamp).format("YYYY/MM/DD")}</p>
                              <p>{dayjs(timeStamp).format("HH:mm:ss.SSS")}</p>
                            </Grid>
                            {i !== e.events.length - 1 && (
                              <Grid item>
                                <ArrowRight />
                              </Grid>
                            )}
                          </React.Fragment>
                        ))}
                    </Grid>
                  </div>
                }
              >
                <TableCell align="center">
                  {e.info?.hostName || "unknown"}
                </TableCell>
                <TableCell align="center">
                  {e.info?.jobName || "unknown"}
                </TableCell>
                <TableCell align="center">
                  <Link to={`/job/${e.info?.jobId}`}>{e.info?.jobId}</Link>
                </TableCell>
                <TableCell align="center">{e.info?.language}</TableCell>
                <TableCell align="center">
                  <Link to={`/node/${e.info?.nodeId}`}>
                    {e.info?.nodeId?.slice(0, 5)}
                  </Link>
                </TableCell>
                <TableCell align="center">{e.info?.pid}</TableCell>
                <TableCell align="center">
                  <CopyableCollapse text={e.info?.workerId || ""} />
                </TableCell>
                <TableCell align="center">
                  {ipLogMap[nodeIpMap[e?.info?.hostName]] && (
                    <Button>
                      <Link
                        target="_blank"
                        to={`/log/${encodeURIComponent(
                          ipLogMap[nodeIpMap[e?.info?.hostName]],
                        )}?fileName=${e?.info?.pid}`}
                      >
                        Log
                      </Link>
                    </Button>
                  )}
                </TableCell>
              </ExpandableTableRow>
            ))}
        </TableBody>
      </Table>
    </Grid>
  );
};

type ActorResourceObj = {
  [key: string]: {
    key: string;
    length: number;
    value: number;
    details: [string, number][];
  };
};

const ActorResource = ({
  requiredResources,
}: {
  requiredResources: { [key: string]: number };
}) => {
  const actorResource = useMemo<ActorResourceObj | null>(() => {
    const kvs = Object.entries(requiredResources || {}).filter(
      ([k]) => !k.startsWith("RAY_JOB_RESOURCE"),
    );
    const values: ActorResourceObj = {};

    if (!kvs.length) {
      return null;
    }
    kvs.forEach(([k, v]) => {
      const resourceKeys = k.split("_");
      const mainKey = resourceKeys[0];

      if (!values[mainKey]) {
        values[mainKey] = {
          key: mainKey,
          length: resourceKeys.length,
          value: v,
          details: [[k, v]],
        };
      } else {
        const { length } = values[mainKey];
        if (length > resourceKeys.length) {
          values[mainKey].length = resourceKeys.length;
          values[mainKey].key = k;
          values[mainKey].value = v;
        }
        values[mainKey].details.push([k, v]);
      }
    });

    return values;
  }, [requiredResources]);

  return (
    actorResource && (
      <React.Fragment>
        <h5>Required Resource</h5>
        <Grid container spacing={2}>
          {Object.entries(actorResource).map(([key, rss]) => (
            <Grid item>
              {key}: {rss.value}{key === 'memory' ? ' bytes' : ''}{" "}
              {rss.length > 1 && (
                <Tooltip
                  title={
                    <pre style={{ whiteSpace: "pre-wrap" }}>
                      {rss.details.map((e) => e.join(": ")).join("\n")}
                    </pre>
                  }
                  interactive
                >
                  <Chip size="small" label="PG" />
                </Tooltip>
              )}
            </Grid>
          ))}
        </Grid>
      </React.Fragment>
    )
  );
};

const ActorTable = ({
  actors = {},
  workers = [],
  isWorker,
}: {
  actors: { [actorId: string]: Actor };
  workers?: Worker[];
  isWorker?: boolean;
}) => {
  const { changeFilter: _changeFilter, filterFunc } = useFilter();
  const { ipLogMap } = useContext(GlobalContext);
  const totalLength = Object.values(actors || {}).length;
  const actorList = Object.values(actors || {})
    .map((e) => {
      const functionDescriptor =
        e.taskSpec?.functionDescriptor || e.functionDescriptor || {};
      return {
        ...e,
        functionDesc: Object.values(
          functionDescriptor.javaFunctionDescriptor ||
          functionDescriptor.pythonFunctionDescriptor ||
          {},
        ).join(" "),
      };
    })
    .filter(filterFunc);
  const { page, pageSize, setPage, startIndex, endIndex, setPageSize } =
    usePage();
  const list = actorList.slice(startIndex, endIndex);
  const changeFilter: typeof _changeFilter = (...params) => {
    _changeFilter(...params);
    setPage(1);
  };

  return (
    <React.Fragment>
      {totalLength > 1 && (
        <React.Fragment>
          <div style={{ flex: 1, display: "flex", alignItems: "center" }}>
            <Autocomplete
              style={{ margin: 8, width: 120 }}
              options={Array.from(
                new Set(Object.values(actors).map((e) => e.state)),
              )}
              onInputChange={(_: any, value: string) => {
                changeFilter("state", value.trim());
              }}
              renderInput={(params: TextFieldProps) => (
                <TextField {...params} label="State" />
              )}
            />
            <Autocomplete
              style={{ margin: 8, width: 150 }}
              options={Array.from(
                new Set(Object.values(actors).map((e) =>
                  e.deathCause?.keyword ? e.deathCause.keyword : '-')),
              )}
              onInputChange={(_: any, value: string) => {
                changeFilter("deathCause.keyword", value.trim());
              }}
              renderInput={(params: TextFieldProps) => (
                <TextField {...params} label="Death Cause" />
              )}
            />
            <Autocomplete
              style={{ margin: 8, width: 150 }}
              options={Array.from(
                new Set(Object.values(actors).map((e) => e.address?.ipAddress)),
              )}
              onInputChange={(_: any, value: string) => {
                changeFilter("address.ipAddress", value.trim());
              }}
              renderInput={(params: TextFieldProps) => (
                <TextField {...params} label="IP" />
              )}
            />
            <TextField
              style={{ margin: 8, width: 120 }}
              label="PID"
              size="small"
              InputProps={{
                onChange: ({ target: { value } }) => {
                  changeFilter("pid", value.trim());
                },
                endAdornment: (
                  <InputAdornment position="end">
                    <SearchOutlined />
                  </InputAdornment>
                ),
              }}
            />
            <TextField
              style={{ margin: 8, width: 200 }}
              label="Task Func Desc"
              size="small"
              InputProps={{
                onChange: ({ target: { value } }) => {
                  changeFilter("functionDesc", value.trim());
                },
                endAdornment: (
                  <InputAdornment position="end">
                    <SearchOutlined />
                  </InputAdornment>
                ),
              }}
            />
            <TextField
              style={{ margin: 8, width: 120 }}
              label="Name"
              size="small"
              InputProps={{
                onChange: ({ target: { value } }) => {
                  changeFilter("name", value.trim());
                },
                endAdornment: (
                  <InputAdornment position="end">
                    <SearchOutlined />
                  </InputAdornment>
                ),
              }}
            />
            <TextField
              style={{ margin: 8, width: 120 }}
              label="Actor ID"
              size="small"
              InputProps={{
                onChange: ({ target: { value } }) => {
                  changeFilter("actorId", value.trim());
                },
                endAdornment: (
                  <InputAdornment position="end">
                    <SearchOutlined />
                  </InputAdornment>
                ),
              }}
            />
            <TextField
              style={{ margin: 8, width: 120 }}
              label="Page Size"
              size="small"
              InputProps={{
                onChange: ({ target: { value } }) => {
                  setPageSize(Math.min(Number(value), 500) || 10);
                },
              }}
            />
          </div>
          <div style={{ display: "flex", alignItems: "center" }}>
            <div>
              <Pagination
                page={page}
                onChange={(e, num) => setPage(num)}
                count={Math.ceil(actorList.length / pageSize)}
              />
            </div>
            <div>
              <StateCounter type="actor" list={actorList} />
            </div>
          </div>
        </React.Fragment>
      )}
      <Table size="small">
        <TableHead>
          <TableRow>
            {[
              "",
              "ID",
              "Restart Times",
              "Name",
              "Class",
              "Function",
              "Job Id",
              "Pid",
              "IP",
              "Port",
              "State",
              "Death Cause",
              "Log",
            ].map((col) => (
              <TableCell align="center" key={col}>
                {col}
              </TableCell>
            ))}
          </TableRow>
        </TableHead>
        <TableBody>
          {list.map(
            ({
              actorId,
              functionDescriptor,
              jobId,
              pid,
              address,
              state,
              name,
              numRestarts,
              deathCause,
              requiredResources,
            }) => (
              <ExpandableTableRow
                length={
                  workers.filter(
                    (e) =>
                      e.pid === pid &&
                      address.ipAddress === e.coreWorkerStats[0].ipAddress,
                  ).length
                }
                expandComponent={
                  <div>
                    <ActorResource requiredResources={requiredResources} />
                    <h5>Pipeline</h5>
                    <ActorPipeline actorId={actorId} />
                    {!isWorker && (
                      <React.Fragment>
                        <h5>Related worker</h5>
                        <RayletWorkerTable
                          actorMap={{}}
                          workers={workers.filter(
                            (e) =>
                              e.pid === pid &&
                              address.ipAddress ===
                              e.coreWorkerStats[0].ipAddress,
                          )}
                          mini
                        />
                      </React.Fragment>
                    )}
                  </div>
                }
                key={actorId}
              >
                <TableCell align="center">
                  <CopyableCollapse text={actorId} />
                </TableCell>
                <TableCell
                  align="center"
                  style={{
                    color: Number(numRestarts) > 0 ? orange[500] : "inherit",
                  }}
                >
                  {numRestarts}
                </TableCell>
                <TableCell align="center">{name}</TableCell>
                <TableCell align="center">
                  {functionDescriptor?.javaFunctionDescriptor?.className}
                  {functionDescriptor?.pythonFunctionDescriptor?.className}
                </TableCell>
                <TableCell align="center">
                  {functionDescriptor?.javaFunctionDescriptor?.functionName}
                  {functionDescriptor?.pythonFunctionDescriptor?.functionName}
                </TableCell>
                <TableCell align="center">{jobId}</TableCell>
                <TableCell align="center">{pid}</TableCell>
                <TableCell align="center">{address?.ipAddress}</TableCell>
                <TableCell align="center">{address?.port}</TableCell>
                <TableCell align="center">
                  <StatusChip type="actor" status={state} />
                </TableCell>
                <TableCell align="center">
                  {deathCause?.keyword
                    ? <Tooltip
                      arrow
                      interactive
                      title={
                        <div>
                          <p>{deathCause.message}</p>
                        </div>
                      }
                    >
                      <div style={{ whiteSpace: 'nowrap', cursor: "pointer" }}>
                        <StatusChip type="deathCause" status={deathCause.keyword} />
                      </div>
                    </Tooltip>
                    : '-'}
                </TableCell>
                <TableCell align="center">
                  {ipLogMap[address?.ipAddress] && (
                    <Button>
                      <Link
                        target="_blank"
                        to={`/log/${encodeURIComponent(
                          ipLogMap[address?.ipAddress],
                        )}?fileName=${pid}`}
                      >
                        Log
                      </Link>
                    </Button>
                  )}
                </TableCell>
              </ExpandableTableRow>
            ),
          )}
        </TableBody>
      </Table>
    </React.Fragment>
  );
};

export default ActorTable;
