import {
  Button,
  Grid,
  makeStyles,
  Switch,
  Tab,
  TableContainer,
  Tabs,
  Tooltip,
} from "@material-ui/core";
import dayjs from "dayjs";
import React from "react";
import { Link, RouteComponentProps } from "react-router-dom";
import ActorTable from "../../components/ActorTable";
import Loading from "../../components/Loading";
import PercentageBar from "../../components/PercentageBar";
import { StatusChip } from "../../components/StatusChip";
import TitleCard from "../../components/TitleCard";
import RayletWorkerTable from "../../components/WorkerTable";
import { memoryConverter } from "../../util/converter";
import { PageTitle, slsDisplayer } from "../../util/func";
import NodeResouce from "./compoments/NodeResouce";
import { useNodeDetail } from "./hook/useNodeDetail";

const useStyle = makeStyles((theme) => ({
  root: {
    padding: theme.spacing(2),
  },
  paper: {
    padding: theme.spacing(2),
    marginTop: theme.spacing(2),
    marginBottom: theme.spacing(2),
  },
  label: {
    fontWeight: "bold",
  },
  tab: {
    marginBottom: theme.spacing(2),
  },
}));

const NodeDetailPage = (props: RouteComponentProps<{ id: string }>) => {
  const classes = useStyle();
  const {
    params,
    selectedTab,
    nodeDetail,
    msg,
    isRefreshing,
    onRefreshChange,
    raylet,
    handleChange,
    namespaceMap,
  } = useNodeDetail(props);
  const cpuCount = nodeDetail?.cpuCount || nodeDetail?.cpus || [];

  return (
    <div className={classes.root}>
      <PageTitle title={`Node ${raylet?.nodeId?.slice(0, 5)}`} />
      <Loading loading={msg.startsWith("Loading")} />
      <TitleCard
        title={
          <div>
            NODE - {params.id}{" "}
            <StatusChip
              type="node"
              status={nodeDetail?.raylet?.state || "LOADING"}
            />{" "}
            <Tooltip title="Auto Refresh" placement="right">
              <Switch
                size="small"
                checked={isRefreshing}
                onChange={onRefreshChange}
                name="refresh"
                inputProps={{ "aria-label": "secondary checkbox" }}
              />
            </Tooltip>
          </div>
        }
      >
        {msg}
      </TitleCard>
      <TitleCard title="Detail">
        <Tabs
          value={selectedTab}
          onChange={handleChange}
          className={classes.tab}
        >
          <Tab value="info" label="Info" />
          <Tab value="resource" label="Resource" />
          <Tab
            value="worker"
            label={`Worker (${nodeDetail?.workers.length || 0})`}
          />
          <Tab
            value="actor"
            label={`Actor (${
              Object.values(nodeDetail?.actors || {}).length || 0
            })`}
          />
        </Tabs>
        {nodeDetail && selectedTab === "info" && (
          <div className={classes.paper}>
            <Grid container spacing={2}>
              {slsDisplayer(nodeDetail.slsUrl).map((e) => (
                <Grid item>
                  <Button>{e}</Button>
                </Grid>
              ))}
              {nodeDetail?.logUrl && [
                <Grid item>
                  <Button>
                    <Link
                      target="_blank"
                      to={`/log/${encodeURIComponent(nodeDetail?.logUrl)}`}
                    >
                      Node Log
                    </Link>
                  </Button>
                </Grid>,
                <Grid item>
                  <Button>
                    <Link
                      target="_blank"
                      to={`/log/${encodeURIComponent(
                        `${nodeDetail?.logUrl}/dashboard_agent.log`,
                      )}`}
                    >
                      Dashboard Agent
                    </Link>
                  </Button>
                </Grid>,
              ]}
            </Grid>
            <Grid container spacing={2}>
              <Grid item xs>
                <div className={classes.label}>Hostname</div>{" "}
                {nodeDetail.hostname}
              </Grid>
              <Grid item xs>
                <div className={classes.label}>IP</div> {nodeDetail.ip}
              </Grid>
            </Grid>
            <Grid container spacing={2}>
              <Grid item xs>
                <div className={classes.label}>CPU (Logic/Physic)</div>{" "}
                {cpuCount[0]} / {cpuCount[1]}
              </Grid>
              <Grid item xs>
                <div className={classes.label}>Load (1/5/15min)</div>{" "}
                {nodeDetail?.loadAvg &&
                  nodeDetail.loadAvg[0] &&
                  nodeDetail.loadAvg[0]
                    .map((e) => Number(e).toFixed(2))
                    .join("/")}
              </Grid>
            </Grid>
            <Grid container spacing={2}>
              <Grid item xs>
                <div className={classes.label}>Load per CPU (1/5/15min)</div>{" "}
                {nodeDetail?.loadAvg &&
                  nodeDetail.loadAvg[1]
                    .map((e) => Number(e).toFixed(2))
                    .join("/")}
              </Grid>
              <Grid item xs>
                <div className={classes.label}>Boot Time</div>{" "}
                {dayjs(nodeDetail.bootTime * 1000).format(
                  "YYYY/MM/DD HH:mm:ss",
                )}
              </Grid>
            </Grid>
            {nodeDetail?.net && (
              <Grid container spacing={2}>
                <Grid item xs>
                  <div className={classes.label}>Sent Tps</div>{" "}
                  {memoryConverter(nodeDetail?.net[0])}/s
                </Grid>
                <Grid item xs>
                  <div className={classes.label}>Recieved Tps</div>{" "}
                  {memoryConverter(nodeDetail?.net[1])}/s
                </Grid>
              </Grid>
            )}

            <Grid container spacing={2}>
              <Grid item xs>
                <div className={classes.label}>Memory</div>{" "}
                {nodeDetail?.mem && (
                  <Tooltip
                    title={
                      <pre
                        style={{
                          whiteSpace: "pre-wrap",
                          wordBreak: "break-word",
                        }}
                      >{`${Object.entries(nodeDetail.mem)
                        .map(([k, v]) => `${k}: ${memoryConverter(v)}`)
                        .join("\n")}`}</pre>
                    }
                    arrow
                    interactive
                  >
                    <div>
                      <PercentageBar
                        num={nodeDetail.mem.used}
                        total={nodeDetail.mem.total}
                      >
                        {memoryConverter(nodeDetail.mem.used)}/
                        {memoryConverter(nodeDetail.mem.total)}(
                        {(
                          (nodeDetail.mem.used / nodeDetail.mem.total) *
                          100
                        ).toFixed(2)}
                        %)
                      </PercentageBar>
                    </div>
                  </Tooltip>
                )}
              </Grid>
              <Grid item xs>
                <div className={classes.label}>CPU</div>{" "}
                <Tooltip
                  arrow
                  interactive
                  title={
                    <pre
                      style={{
                        whiteSpace: "pre-wrap",
                        wordBreak: "break-word",
                      }}
                    >{`CPU Count(Logic/Physic): ${nodeDetail?.cpuCount?.join(
                      "/",
                    )}\n\n---CPU Times---\n\n${Object.entries(
                      nodeDetail?.cpuTimesPercent || {},
                    )
                      .map(([k, v]) => `${k}: \t${v}%`)
                      .join("\n")}`}</pre>
                  }
                >
                  <div>
                    <PercentageBar
                      num={Number(nodeDetail?.cpu || nodeDetail?.cpuPercent)}
                      total={100}
                    >
                      {nodeDetail?.cpu || nodeDetail?.cpuPercent}%
                    </PercentageBar>
                  </div>
                </Tooltip>
              </Grid>
            </Grid>
            <Grid container spacing={2}>
              {nodeDetail?.disk &&
                Object.entries(nodeDetail?.disk).map(([path, obj]) => (
                  <Grid item xs={6} key={path}>
                    <div className={classes.label}>Disk ({path})</div>{" "}
                    {obj && (
                      <PercentageBar num={Number(obj.used)} total={obj.total}>
                        {memoryConverter(obj.used)}/{memoryConverter(obj.total)}
                        ({obj.percent}%, {memoryConverter(obj.free)} free)
                      </PercentageBar>
                    )}
                  </Grid>
                ))}
            </Grid>
            <Grid container spacing={2}>
              <Grid item xs>
                <div className={classes.label}>Namespaces</div>{" "}
                {namespaceMap[nodeDetail.hostname]?.join(",")}
              </Grid>
              <Grid item xs>
                <div className={classes.label}>Namespaces</div>{" "}
                {namespaceMap[nodeDetail.hostname]?.join(",")}
              </Grid>
            </Grid>
          </div>
        )}
        {selectedTab === "resource" && <NodeResouce nodeId={params.id} />}
        {nodeDetail?.workers && selectedTab === "worker" && (
          <React.Fragment>
            <TableContainer className={classes.paper}>
              <RayletWorkerTable
                workers={nodeDetail?.workers}
                actorMap={nodeDetail?.actors}
                entity="node"
                entityId={nodeDetail.raylet.nodeId}
              />
            </TableContainer>
          </React.Fragment>
        )}
        {nodeDetail?.actors && selectedTab === "actor" && (
          <React.Fragment>
            <TableContainer className={classes.paper}>
              <ActorTable
                actors={nodeDetail.actors}
                workers={nodeDetail?.workers}
              />
            </TableContainer>
          </React.Fragment>
        )}
      </TitleCard>
    </div>
  );
};

export default NodeDetailPage;
