import {
  Button,
  Grid,
  IconButton,
  Tab,
  Table,
  TableBody,
  TableCell,
  TableContainer,
  TableHead,
  TableRow,
  Tabs,
  Tooltip,
} from "@material-ui/core";
import {
  ArrowRight,
  KeyboardArrowDown,
  KeyboardArrowRight,
} from "@material-ui/icons";
import Pagination from "@material-ui/lab/Pagination";
import dayjs from "dayjs";
import React, {
  PropsWithChildren,
  ReactNode,
  useContext,
  useEffect,
  useState,
} from "react";
import { Link } from "react-router-dom";
import { GlobalContext } from "../App";
import { getWorkerPipelineEvents } from "../service/event";
import { Actor } from "../type/actor";
import { EventPiplineRsp } from "../type/event";
import { CoreWorkerStats, Worker } from "../type/worker";
import { memoryConverter } from "../util/converter";
import { slsDisplayer } from "../util/func";

import { useFilter, useSorter } from "../util/hook";
import ActorTable from "./ActorTable";
import PercentageBar from "./PercentageBar";
import { SearchInput } from "./SearchComponent";
import { StatusChip } from "./StatusChip";
import { SortButton } from './SortButton';
import { rest } from "lodash";

export const ExpandableTableRow = ({
  children,
  expandComponent,
  length,
  stateKey = "",
  ...otherProps
}: PropsWithChildren<{
  expandComponent: ReactNode;
  length: number;
  stateKey?: string;
}>) => {
  const [isExpanded, setIsExpanded] = React.useState(false);

  useEffect(() => {
    if (stateKey.startsWith("ON")) {
      setIsExpanded(true);
    } else if (stateKey.startsWith("OFF")) {
      setIsExpanded(false);
    }
  }, [stateKey]);

  return (
    <React.Fragment>
      <TableRow {...otherProps}>
        <TableCell padding="checkbox">
          <IconButton
            style={{ color: "inherit" }}
            onClick={() => setIsExpanded(!isExpanded)}
          >
            {length > 1 && length}
            {isExpanded ? <KeyboardArrowDown /> : <KeyboardArrowRight />}
          </IconButton>
        </TableCell>
        {children}
      </TableRow>
      {isExpanded && (
        <TableRow>
          <TableCell colSpan={24}>{expandComponent}</TableCell>
        </TableRow>
      )}
    </React.Fragment>
  );
};

const WorkerDetailTable = ({
  actorMap,
  coreWorkerStats,
}: {
  actorMap: { [actorId: string]: Actor };
  coreWorkerStats: CoreWorkerStats[];
}) => {
  const actors = {} as { [actorId: string]: Actor };
  (coreWorkerStats || [])
    .filter((e) => actorMap[e.actorId])
    .forEach((e) => (actors[e.actorId] = actorMap[e.actorId]));

  if (!Object.values(actors).length) {
    return <p>The Worker Haven't Had Related Actor Yet.</p>;
  }

  return (
    <TableContainer>
      <ActorTable actors={actors} />
    </TableContainer>
  );
};

const RayletWorkerTable = ({
  workers = [],
  actorMap,
  mini,
  entity,
  entityId,
}: {
  workers: Worker[];
  actorMap: { [actorId: string]: Actor };
  mini?: boolean;
  entity?: "job" | "node";
  entityId?: string;
}) => {
  const { changeFilter: _changeFileter, filterFunc } = useFilter();
  const [page, setPage] = useState({ pageSize: 10, pageNo: 1 });
  const [key, setKey] = useState("");
  const [historyMode, setHistoryMode] = useState(false);
  const [historyEvent, setHistoryEvents] = useState<EventPiplineRsp["data"]>();
  const { nodeMap, ipLogMap, nodeIpMap } = useContext(GlobalContext);
  const open = () => setKey(`ON${Math.random()}`);
  const close = () => setKey(`OFF${Math.random()}`);
  const changeFilter: typeof _changeFileter = (...params) => {
    _changeFileter(...params);
    setPage({ ...page, pageNo: 1 });
  };
  useEffect(() => {
    setPage({ pageSize: 10, pageNo: 1 });
    if (historyMode && entity && entityId) {
      getWorkerPipelineEvents(entityId, entity)?.then((res) => {
        if (res && res.data) {
          setHistoryEvents(res.data.data);
        }
      });
    }
  }, [historyMode, entity, entityId]);
  const { sorterFunc, setOrderDesc, setSortKey, sorterKey, desc } = useSorter();
  const commonSort = { sorterKey, setSortKey, desc, setOrderDesc };

  const renderSortKey = (col: string, sorterCols: { key: string, target: string }[]) => {
    const sortCol = sorterCols.find(({ key }) => key === col)

    if (sortCol) {
      return <SortButton
        {...commonSort}
        title={col}
        target={sortCol.target}
      />
    }
    return col;
  }

  const renderRuntime = () => {
    const sorterCols = [{ key: 'CPU', target: 'cpuPercent' }, { key: 'Memory', target: 'memoryRss' }]
    const sorterWorks = workers
      .filter(filterFunc)
      .slice(
        (page.pageNo - 1) * page.pageSize,
        page.pageNo * page.pageSize,
      )
      .map(({ memoryInfo, ...rest }) => ({ ...rest, memoryInfo, memoryRss: memoryInfo.rss }))
      .sort((aWorker, bWorker) => {
        const a =
          (aWorker.coreWorkerStats || []).filter(
            (e) => actorMap[e.actorId],
          ).length || 0;
        const b =
          (bWorker.coreWorkerStats || []).filter(
            (e) => actorMap[e.actorId],
          ).length || 0;
        return b - a;
      }).sort(sorterFunc)

    return (
      <div>
        {!mini && (
          <div>
            <div style={{ display: "flex", alignItems: "center" }}>
              <SearchInput
                label="Pid"
                onChange={(value) => changeFilter("pid", value)}
              />
              <Button onClick={open}>Expand All</Button>
              <Button onClick={close}>Collapse All</Button>
            </div>
          </div>
        )}
        <Table size="small">
          <TableHead>
            <TableRow>
              {[
                "",
                "Pid",
                "CPU",
                "Memory",
                "Create Time",
                "Log",
                "Ops",
                "IP/Hostname",
              ].map((col) => (
                <TableCell align="center" key={col}>
                  {renderSortKey(col, sorterCols)}
                </TableCell>
              ))}
            </TableRow>
          </TableHead>
          <TableBody>
            {sorterWorks
              .map(
                ({
                  pid,
                  cpuPercent,
                  cpuTimes,
                  memoryInfo,
                  createTime,
                  coreWorkerStats = [],
                  slsUrl,
                  language,
                  ip,
                  hostname,
                }) => (
                  <ExpandableTableRow
                    expandComponent={
                      <WorkerDetailTable
                        actorMap={actorMap}
                        coreWorkerStats={coreWorkerStats}
                      />
                    }
                    length={
                      (coreWorkerStats || []).filter(
                        (e) => actorMap[e.actorId],
                      ).length
                    }
                    key={pid}
                    stateKey={key}
                  >
                    <TableCell align="center">{pid}</TableCell>
                    <TableCell align="center">
                      <Tooltip
                        title={
                          <pre style={{ overflow: "auto" }}>
                            -----CPU TIMES-----
                            {Object.entries(cpuTimes || {}).map(
                              ([key, val]) => (
                                <div style={{ margin: 4 }}>
                                  {key}:{val}
                                </div>
                              ),
                            )}
                          </pre>
                        }
                        interactive
                      >
                        <div>
                          <PercentageBar
                            num={Number(cpuPercent)}
                            total={100}
                          >
                            {cpuPercent}%
                          </PercentageBar>
                        </div>
                      </Tooltip>
                    </TableCell>
                    <TableCell align="center">
                      <Tooltip
                        title={
                          <pre>
                            {Object.entries(memoryInfo || {}).map(
                              ([key, val]) => (
                                <div style={{ margin: 4 }}>
                                  {key}:{memoryConverter(val)}
                                </div>
                              ),
                            )}
                          </pre>
                        }
                      >
                        <div>{memoryConverter(memoryInfo?.rss)}</div>
                      </Tooltip>
                    </TableCell>
                    <TableCell align="center">
                      {dayjs(createTime * 1000).format(
                        "YYYY/MM/DD HH:mm:ss",
                      )}
                    </TableCell>
                    <TableCell align="center">
                      {slsUrl && <Button>{slsDisplayer(slsUrl)}</Button>}
                      {ipLogMap[ip] && (
                        <Button>
                          <Link
                            target="_blank"
                            to={`/log/${encodeURIComponent(
                              ipLogMap[ip],
                            )}?fileName=${pid}`}
                          >
                            Log
                          </Link>
                        </Button>
                      )}
                    </TableCell>
                    <TableCell align="center">
                      {language === "JAVA" && (
                        <div>
                          <Button
                            onClick={() => {
                              window.open(
                                `#/cmd/jstack/${coreWorkerStats[0]?.ipAddress}/${pid}`,
                              );
                            }}
                          >
                            jstack
                          </Button>{" "}
                          <Button
                            onClick={() => {
                              window.open(`#/cmd/jmap/${ip}/${pid}`);
                            }}
                          >
                            jmap
                          </Button>
                          <Button
                            onClick={() => {
                              window.open(`#/cmd/jstat/${ip}/${pid}`);
                            }}
                          >
                            jstat
                          </Button>
                        </div>
                      )}
                      {language === "PYTHON" && (
                        <div>
                          <Button
                            onClick={() => {
                              window.open(`#/cmd/pystack/${ip}/${pid}`);
                            }}
                          >
                            pystack
                          </Button>
                        </div>
                      )}
                      {language === "CPP" && (
                        <div>
                          <Button
                            onClick={() => {
                              window.open(`#/cmd/pstack/${ip}/${pid}`);
                            }}
                          >
                            pstack
                          </Button>
                        </div>
                      )}
                      <Button
                        onClick={() => {
                          window.open(`#/profiler/${ip}/${pid}`);
                        }}
                      >
                        profiler
                      </Button>
                      {language === "PYTHON" && (<Button
                        onClick={() => {
                          window.open(`#/pyprofiler/${ip}/${pid}`);
                        }}
                      >
                        pyprofiler
                      </Button>)}
                    </TableCell>
                    <TableCell align="center">
                      {ip}
                      <br />
                      {nodeMap[hostname] ? (
                        <Link
                          target="_blank"
                          to={`/node/${nodeMap[hostname]}`}
                        >
                          {hostname}
                        </Link>
                      ) : (
                        hostname
                      )}
                    </TableCell>
                  </ExpandableTableRow>
                ),
              )}
          </TableBody>
        </Table>
      </div>
    )
  }

  const renderHistory = () => {
    return (
      <div>
        <Table>
          <TableHead>
            <TableRow>
              {[
                "",
                "Host Name",
                "Job Name",
                "Job Id",
                "Node",
                "Pid",
                "WorkerId",
                "Log",
              ].map((col) => (
                <TableCell align="center" key={col}>
                  {col}
                </TableCell>
              ))}
            </TableRow>
          </TableHead>
          <TableBody>
            {Object.entries(historyEvent?.events || {})
              .filter(filterFunc)
              .slice(
                (page.pageNo - 1) * page.pageSize,
                page.pageNo * page.pageSize,
              )
              .map(([key, { 1: e }]) => (
                <ExpandableTableRow
                  length={1}
                  key={key}
                  expandComponent={
                    <div>
                      <Grid
                        container
                        alignItems="center"
                        justify="space-around"
                      >
                        {e &&
                          e
                            .sort(
                              (a, b) =>
                                dayjs(a.timeStamp).valueOf() -
                                dayjs(b.timeStamp).valueOf(),
                            )
                            .map(({ label, timeStamp, severity }, i) => (
                              <React.Fragment>
                                <Grid item style={{ textAlign: "center" }}>
                                  <p>
                                    {
                                      <StatusChip
                                        status={label}
                                        type={severity}
                                      />
                                    }
                                  </p>
                                  <p>
                                    {dayjs(timeStamp).format(
                                      "HH:mm:ss.SSS",
                                    )}
                                  </p>
                                </Grid>
                                {i !== e.length - 1 && (
                                  <Grid item>
                                    <ArrowRight />
                                  </Grid>
                                )}
                              </React.Fragment>
                            ))}
                      </Grid>
                      <ActorTable
                        isWorker
                        actors={Object.fromEntries(
                          Object.entries(actorMap).filter(([k, v]) => {
                            return (
                              v?.address?.workerId &&
                              e[0]?.customFields?.workerId &&
                              v?.address?.workerId ===
                              e[0]?.customFields?.workerId
                            );
                          }),
                        )}
                      />
                    </div>
                  }
                >
                  <TableCell align="center">
                    {e[0]?.customFields?.hostName}
                  </TableCell>
                  <TableCell align="center">{e[0]?.jobName}</TableCell>
                  <TableCell align="center">
                    <Link to={`/job/${e[0]?.jobId}`}>{e[0]?.jobId}</Link>
                  </TableCell>
                  <TableCell align="center">
                    <Link to={`/node/${e[0]?.nodeId}`}>
                      {e[0]?.nodeId?.slice(0, 5)}
                    </Link>
                  </TableCell>
                  <TableCell align="center">
                    {e[0]?.customFields?.pid}
                  </TableCell>
                  <TableCell align="center">{key}</TableCell>
                  <TableCell align="center">
                    {ipLogMap[nodeIpMap[e[0]?.customFields?.hostName]] && (
                      <Button>
                        <Link
                          target="_blank"
                          to={`/log/${encodeURIComponent(
                            ipLogMap[
                            nodeIpMap[e[0]?.customFields?.hostName]
                            ],
                          )}?fileName=${e[0]?.customFields?.pid}`}
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
      </div>
    )
  }

  return (
    <div>
      <Tabs
        value={historyMode ? 1 : 0}
        onChange={(evt, val) => {
          setHistoryMode(val === 1);
        }}
      >
        <Tab label="runtime" />
        <Tab label="history" />
      </Tabs>
      <div>
        <div>
          <Pagination
            page={page.pageNo}
            onChange={(e, num) => setPage({ ...page, pageNo: num })}
            count={Math.ceil(
              (historyMode
                ? Object.entries(historyEvent?.events || {}).filter(filterFunc)
                  .length
                : workers.filter(filterFunc).length) / page.pageSize,
            )}
          />
        </div>
        {historyMode ? renderHistory() : renderRuntime()}
      </div>
    </div>
  );
};

export default RayletWorkerTable;
