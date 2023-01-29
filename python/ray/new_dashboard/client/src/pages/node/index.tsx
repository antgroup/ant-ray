import {
  Button,
  ButtonGroup,
  Chip,
  Grid,
  MenuItem,
  Paper,
  Switch,
  Table,
  TableBody,
  TableCell,
  TableContainer,
  TableHead,
  TablePagination,
  TableRow,
  Tooltip,
} from "@material-ui/core";
import { makeStyles } from "@material-ui/core/styles";
import { ArrowDownward, ArrowUpward } from "@material-ui/icons";
import Pagination from "@material-ui/lab/Pagination";
import dayjs from "dayjs";
import React from "react";
import { Link } from "react-router-dom";
import Loading from "../../components/Loading";
import PercentageBar from "../../components/PercentageBar";
import { SearchInput, SearchSelect } from "../../components/SearchComponent";
import StateCounter from "../../components/StatesCounter";
import { StatusChip } from "../../components/StatusChip";
import TitleCard from "../../components/TitleCard";
import { NodeDetail } from "../../type/node";
import { memoryConverter } from "../../util/converter";
import { ButtonMenuList, PageTitle, slsDisplayer } from "../../util/func";
import { useNodeList } from "./hook/useNodeList";

const useStyles = makeStyles((theme) => ({
  root: {
    padding: theme.spacing(2),
    width: "100%",
    position: "relative",
  },
  fullGrid: {
    width: "100%",
  },
}));

export const brpcLinkChanger = (href: string) => {
  const { location } = window;
  const { pathname } = location;
  const pathArr = pathname.split("/");
  if (pathArr.some((e) => e.split(".").length > 1)) {
    const index = pathArr.findIndex((e) => e.includes("."));
    const resultArr = pathArr.slice(0, index);
    if (href.includes("://")) {
      resultArr.push(href.split("://").pop() || "");
    } else {
      resultArr.push(href);
    }
    return `${location.protocol}//${location.host}${resultArr.join("/")}`;
  }
  if (href.startsWith("http")) {
    return href;
  }

  return `http://${href}`;
};

export const NodeCard = (props: { node: NodeDetail }) => {
  const { node } = props;

  if (!node) {
    return null;
  }

  const {
    raylet,
    hostname,
    ip,
    cpu: oldcpu,
    cpuPercent,
    mem,
    net,
    disk,
    slsUrl,
    logUrl,
    cpuCount,
    cpuTimesPercent,
  } = node;
  const { nodeId, state, brpcPort } = raylet;
  const cpu = oldcpu || cpuPercent || 0;

  return (
    <Paper variant="outlined" style={{ padding: "12px 12px", margin: 12 }}>
      <p style={{ fontWeight: "bold", fontSize: 12, textDecoration: "none" }}>
        <Link to={`node/${nodeId}`}>{nodeId}</Link>{" "}
      </p>
      <p>
        <Grid container spacing={1}>
          <Grid item>
            <StatusChip type="node" status={state} />
          </Grid>
          <Grid item>
            {hostname}({ip})
          </Grid>
          {net && net[0] >= 0 && (
            <Grid item>
              <span style={{ fontWeight: "bold" }}>Sent</span>{" "}
              {memoryConverter(net[0])}/s{" "}
              <span style={{ fontWeight: "bold" }}>Received</span>{" "}
              {memoryConverter(net[1])}/s
            </Grid>
          )}
        </Grid>
      </p>
      <Grid container spacing={1} alignItems="baseline">
        {cpu >= 0 && (
          <Grid item xs>
            CPU
            <Tooltip
              arrow
              interactive
              title={
                <pre
                  style={{
                    whiteSpace: "pre-wrap",
                    wordBreak: "break-word",
                  }}
                >{`CPU Count(Logic/Physic): ${cpuCount?.join(
                  "/",
                )}\n\n---CPU Times---\n\n${Object.entries(cpuTimesPercent || {})
                  .map(([k, v]) => `${k}: \t${v}%`)
                  .join("\n")}`}</pre>
              }
            >
              <div>
                <PercentageBar num={Number(cpu || cpuPercent)} total={100}>
                  {cpu || cpuPercent}%
                </PercentageBar>
              </div>
            </Tooltip>
          </Grid>
        )}
        {mem && (
          <Grid item xs>
            Memory
            <Tooltip
              title={`Used(${memoryConverter(
                mem.used,
              )}) = Total(${memoryConverter(
                mem.total,
              )}) - Available(${memoryConverter(
                mem.available,
              )}) - Cached(${memoryConverter(
                mem.cached,
              )}) - Buffers(${memoryConverter(mem.buffers)})`}
              arrow
            >
              <div>
                <PercentageBar
                  num={
                    mem.total -
                    mem.available -
                    (mem.cached || 0) -
                    (mem.buffers || 0)
                  }
                  total={mem.total}
                >
                  {memoryConverter(
                    mem.total -
                    mem.available -
                    (mem.cached || 0) -
                    (mem.buffers || 0),
                  )}
                  /{memoryConverter(mem.total)}(
                  {(
                    ((mem.total -
                      mem.available -
                      (mem.cached || 0) -
                      (mem.buffers || 0)) /
                      mem.total) *
                    100
                  ).toFixed(2)}
                  %)
                </PercentageBar>
              </div>
            </Tooltip>
          </Grid>
        )}
        {disk && disk["/"] && (
          <Grid item xs>
            Disk('/')
            <PercentageBar num={Number(disk["/"].used)} total={disk["/"].total}>
              {memoryConverter(disk["/"].used)}/
              {memoryConverter(disk["/"].total)}({disk["/"].percent}%)
            </PercentageBar>
          </Grid>
        )}
      </Grid>
      <Grid container justify="flex-end" spacing={1} style={{ margin: 8 }}>
        {Object.entries(slsUrl || {}).map(([key, value]) => (
          <Grid>
            <Button href={value} target="_blank" rel="noopener noreferrer">
              {key}.log
            </Button>
          </Grid>
        ))}
        <Grid>
          <Button
            target="_blank"
            rel="noopener noreferrer"
            href={brpcLinkChanger(`${ip}:${raylet.brpcPort}`)}
          >
            BRPC {brpcPort}
          </Button>
        </Grid>
        <Grid>
          <Button>
            <Link to={`/log/${encodeURIComponent(logUrl)}`}>log</Link>
          </Button>
        </Grid>
      </Grid>
    </Paper>
  );
};

export const SortButton = ({
  target,
  title,
  sorterKey,
  desc,
  setSortKey,
  setOrderDesc,
}: {
  target: string;
  title: string;
  sorterKey: string;
  desc: boolean;
  setSortKey: (str: string) => void;
  setOrderDesc: (desc: boolean) => void;
}) => {
  const isSorting = target === sorterKey;
  const onClick = () => {
    if (isSorting) {
      if (desc) {
        setOrderDesc(false);
        setSortKey("");
      } else {
        setOrderDesc(true);
      }
    } else {
      setOrderDesc(false);
      setSortKey(target);
    }
  };
  return (
    <Chip
      size="small"
      onClick={onClick}
      label={title}
      icon={isSorting ? !desc ? <ArrowUpward /> : <ArrowDownward /> : undefined}
      color={isSorting ? "primary" : "default"}
    />
  );
};

const Nodes = () => {
  const classes = useStyles();
  const {
    msg,
    isRefreshing,
    onSwitchChange,
    nodeList,
    changeFilter,
    page,
    setPage,
    setSortKey,
    setOrderDesc,
    mode,
    setMode,
    sorterKey,
    desc,
    includeInactive,
    setIncludeInactive,
  } = useNodeList();
  const commonSort = { sorterKey, setSortKey, desc, setOrderDesc };

  return (
    <div className={classes.root}>
      <PageTitle title="Nodes List" />
      <Loading loading={msg.startsWith("Loading")} />
      <TitleCard
        title={
          <Grid spacing={2} container>
            <Grid item>NODES</Grid>
            <Grid item>
              Auto Refresh:
              <Tooltip title="Auto Refresh" placement="right">
                <Switch
                  size="small"
                  checked={isRefreshing}
                  onChange={onSwitchChange}
                  name="refresh"
                  inputProps={{ "aria-label": "secondary checkbox" }}
                />
              </Tooltip>
            </Grid>
            <Grid item>
              Inactive:
              <Tooltip title="include inactive nodes" placement="right">
                <Switch
                  size="small"
                  checked={includeInactive}
                  onChange={({ target }) => setIncludeInactive(target.checked)}
                  name="inactive"
                  inputProps={{ "aria-label": "secondary checkbox" }}
                />
              </Tooltip>
            </Grid>
            <Grid item>
              <StateCounter type="node" list={nodeList} />
            </Grid>
          </Grid>
        }
      >
        {msg}
      </TitleCard>
      <TitleCard
        title={
          <div>
            List{" "}
            <ButtonGroup size="small">
              <Button
                onClick={() => setMode("table")}
                color={mode === "table" ? "primary" : "default"}
              >
                Table
              </Button>
              <Button
                onClick={() => setMode("card")}
                color={mode === "card" ? "primary" : "default"}
              >
                Card
              </Button>
            </ButtonGroup>
          </div>
        }
      >
        {mode === "table" && (
          <TableContainer style={{ overflowX: "initial" }}>
            <Table size="small">
              <TableHead>
                <TableRow>
                  <TableCell align="center">
                    <SearchSelect
                      label="State"
                      onChange={(value) => changeFilter("state", value.trim())}
                      options={["ALIVE", "DEAD"]}
                    />
                  </TableCell>
                  <TableCell align="center">
                    <SearchInput
                      disableIcon
                      label="Id"
                      onChange={(value) =>
                        changeFilter("raylet.nodeId", value.trim())
                      }
                    />
                  </TableCell>
                  <TableCell align="center">
                    <SearchInput
                      label="Host"
                      onChange={(value) =>
                        changeFilter("hostname", value.trim())
                      }
                    />
                  </TableCell>
                  <TableCell align="center">
                    <SearchInput
                      label="IP"
                      onChange={(value) => changeFilter("ip", value.trim())}
                    />
                  </TableCell>
                  <TableCell align="center">
                    Resource Usage
                    <br />
                    <SortButton
                      {...commonSort}
                      title="CPU"
                      target="cpuPercent"
                    />{" "}
                    <SortButton
                      {...commonSort}
                      title="Memory"
                      target="mem.percent"
                    />{" "}
                    <SortButton
                      {...commonSort}
                      title="Disk"
                      target="disk./.used"
                    />
                  </TableCell>
                  <TableCell align="center">
                    IO
                    <br />
                    <SortButton
                      {...commonSort}
                      title="Sent"
                      target="net[0]"
                    />{" "}
                    <SortButton
                      {...commonSort}
                      title="Received"
                      target="net[1]"
                    />
                  </TableCell>
                  <TableCell align="center">
                    Time
                    <br />
                    <SortButton
                      {...commonSort}
                      title="StartTime"
                      target="raylet.startTime"
                    />
                  </TableCell>
                  <TableCell align="center">Links</TableCell>
                </TableRow>
              </TableHead>
              <TableBody>
                {nodeList
                  .slice(
                    (page.pageNo - 1) * page.pageSize,
                    page.pageNo * page.pageSize,
                  )
                  .map(
                    (
                      {
                        hostname = "",
                        ip = "",
                        cpu,
                        cpuPercent,
                        cpuCount,
                        cpuTimesPercent,
                        mem = {
                          active: 0,
                          available: 0,
                          buffers: 0,
                          cached: 0,
                          free: 0,
                          inactive: 0,
                          percent: 0,
                          shared: 0,
                          slab: 0,
                          total: 0,
                          used: 0,
                        },
                        disk,
                        net = [0, 0],
                        raylet,
                        logUrl,
                        slsUrl,
                        timestamp,
                      }: NodeDetail,
                      i,
                    ) => (
                      <TableRow key={hostname + i}>
                        <TableCell>
                          <StatusChip type="node" status={raylet.state} />
                        </TableCell>
                        <TableCell align="center">
                          <Tooltip title={raylet.nodeId} arrow interactive>
                            <Link to={`/node/${raylet.nodeId}`}>
                              {raylet.nodeId.slice(0, 5)}
                            </Link>
                          </Tooltip>
                        </TableCell>
                        <TableCell align="center">{hostname}</TableCell>
                        <TableCell align="center">{ip}</TableCell>
                        <TableCell>
                          <Grid container spacing={2} direction="row">
                            <Grid className={classes.fullGrid} item>
                              <Tooltip
                                arrow
                                interactive
                                title={
                                  <pre
                                    style={{
                                      whiteSpace: "pre-wrap",
                                      wordBreak: "break-word",
                                    }}
                                  >{`CPU Count(Logic/Physic): ${cpuCount?.join(
                                    "/",
                                  )}\n\n---CPU Times---\n\n${Object.entries(
                                    cpuTimesPercent || {},
                                  )
                                    .map(([k, v]) => `${k}: \t${v}%`)
                                    .join("\n")}`}</pre>
                                }
                              >
                                <div>
                                  <PercentageBar
                                    num={Number(cpu || cpuPercent)}
                                    total={100}
                                  >
                                    CPU: {cpu || cpuPercent}%
                                  </PercentageBar>
                                </div>
                              </Tooltip>
                            </Grid>
                            <Grid className={classes.fullGrid} item>
                              <Tooltip
                                title={
                                  <pre
                                    style={{
                                      whiteSpace: "pre-wrap",
                                      wordBreak: "break-word",
                                    }}
                                  >{`${Object.entries(mem)
                                    .map(
                                      ([k, v]) => `${k}: ${memoryConverter(v)}`,
                                    )
                                    .join("\n")}`}</pre>
                                }
                                arrow
                                interactive
                              >
                                <div>
                                  <PercentageBar
                                    num={mem.used}
                                    total={mem.total}
                                  >
                                    Memory: {memoryConverter(mem.used)}/
                                    {memoryConverter(mem.total)}(
                                    {((mem.used / mem.total) * 100).toFixed(2)}
                                    %)
                                  </PercentageBar>
                                </div>
                              </Tooltip>
                            </Grid>
                            <Grid className={classes.fullGrid} item>
                              <Tooltip
                                title={
                                  <pre
                                    style={{
                                      whiteSpace: "pre-wrap",
                                      wordBreak: "break-word",
                                    }}
                                  >
                                    {Object.entries(disk || {})
                                      .map(
                                        ([key, val]) =>
                                          `${key}: ${memoryConverter(
                                            val.used,
                                          )}/${memoryConverter(val.total)}(${val.percent
                                          }%)`,
                                      )
                                      .join("\n")}
                                  </pre>
                                }
                              >
                                <div>
                                  {disk && disk["/"] && (
                                    <PercentageBar
                                      num={Number(disk["/"].used)}
                                      total={disk["/"].total}
                                    >
                                      Disk('/'):{" "}
                                      {memoryConverter(disk["/"].used)}/
                                      {memoryConverter(disk["/"].total)}(
                                      {disk["/"].percent}%)
                                    </PercentageBar>
                                  )}
                                </div>
                              </Tooltip>
                            </Grid>
                          </Grid>
                        </TableCell>
                        <TableCell align="center">
                          <Grid
                            container
                            spacing={1}
                            direction="row"
                            justify="center"
                          >
                            <Grid item>
                              <StatusChip type="" status="Sent" />
                            </Grid>
                            <Grid item>{memoryConverter(net[0])}/s</Grid>
                            <Grid item>
                              <StatusChip type="" status="Received" />
                            </Grid>
                            <Grid item>{memoryConverter(net[1])}/s</Grid>
                          </Grid>
                        </TableCell>

                        <TableCell align="center">
                          {!!raylet.startTime && (
                            <p>
                              Start Time:{" "}
                              {dayjs(raylet.startTime * 1000).format(
                                "YYYY/MM/DD HH:mm:ss",
                              )}
                            </p>
                          )}
                          {!!raylet.terminateTime && (
                            <p>
                              End Time:{" "}
                              {dayjs(raylet.terminateTime * 1000).format(
                                "YYYY/MM/DD HH:mm:ss",
                              )}
                            </p>
                          )}
                          {raylet.state === 'DEAD' && (
                            <p>
                              Dead Time:{" "}
                              {dayjs(timestamp?.nodeStats * 1000).format(
                                "YYYY/MM/DD HH:mm:ss",
                              )}
                            </p>
                          )}
                        </TableCell>
                        <TableCell>
                          <ButtonMenuList
                            name="Link List"
                            list={[
                              <MenuItem>
                                <Link to={`/log/${encodeURIComponent(logUrl)}`}>
                                  Node Log Index
                                </Link>
                              </MenuItem>,
                              ...slsDisplayer(slsUrl).map((e) => (
                                <MenuItem>{e}</MenuItem>
                              )),
                              raylet.brpcPort && (
                                <MenuItem>
                                  <a
                                    target="_blank"
                                    rel="noopener noreferrer"
                                    href={brpcLinkChanger(
                                      `${ip}:${raylet.brpcPort}`,
                                    )}
                                  >
                                    BRPC {raylet.brpcPort}
                                  </a>
                                </MenuItem>
                              ),
                            ]}
                          />
                        </TableCell>
                      </TableRow>
                    ),
                  )}
              </TableBody>
              <TablePagination
                count={nodeList.length}
                page={page.pageNo - 1}
                rowsPerPage={page.pageSize}
                onPageChange={(evt, page) => setPage("pageNo", page + 1)}
                onRowsPerPageChange={({ target: { value } }) =>
                  setPage("pageSize", Number(value))
                }
              />
            </Table>
          </TableContainer>
        )}
        {mode === "card" && (
          <React.Fragment>
            <div>
              <Pagination
                count={Math.ceil(nodeList.length / page.pageSize)}
                page={page.pageNo}
                onChange={(e, pageNo) => setPage("pageNo", pageNo)}
              />
            </div>
            <Grid container>
              {nodeList
                .slice(
                  (page.pageNo - 1) * page.pageSize,
                  page.pageNo * page.pageSize,
                )
                .map((e) => (
                  <Grid item xs={6}>
                    <NodeCard node={e} />
                  </Grid>
                ))}
            </Grid>
          </React.Fragment>
        )}
      </TitleCard>
    </div>
  );
};

export default Nodes;
