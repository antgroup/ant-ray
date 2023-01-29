import {
  Button,
  Grid,
  makeStyles,
  Switch,
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
import React from "react";
import { Link, RouteComponentProps } from "react-router-dom";
import ActorTable from "../../components/ActorTable";
import EventTable from "../../components/EventTable";
import JobPipeline from "../../components/JobPipeline";
import JobResult from "../../components/JobResult";
import Loading from "../../components/Loading";
import { StatusChip } from "../../components/StatusChip";
import TitleCard from "../../components/TitleCard";
import RayletWorkerTable from "../../components/WorkerTable";
import {
  jsonFormat,
  longTextCut,
  PageTitle,
  slsDisplayer,
} from "../../util/func";
import JobResouce from "./compoments/JobResouce";
import { useJobDetail } from "./hook/useJobDetail";

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
  pageMeta: {
    padding: theme.spacing(2),
    marginTop: theme.spacing(2),
  },
  tab: {
    marginBottom: theme.spacing(2),
  },
  dependenciesChip: {
    margin: theme.spacing(0.5),
    wordBreak: "break-all",
  },
  alert: {
    color: theme.palette.error.main,
  },
}));

const JobDetailPage = (props: RouteComponentProps<{ id: string }>) => {
  const classes = useStyle();
  const {
    actorMap,
    jobInfo,
    job,
    msg,
    selectedTab,
    handleChange,
    handleSwitchChange,
    params,
    refreshing,
    ipLogMap,
  } = useJobDetail(props);

  if (!job || !jobInfo || !job.jobInfo.name) {
    return (
      <div className={classes.root}>
        <PageTitle title={`JOB - ${params.id}`} />
        <Loading loading={msg.startsWith("Loading")} />
        <TitleCard title={`JOB - ${params.id}`}>
          <StatusChip type="job" status="LOADING" />
          <br />
          Auto Refresh:
          <Switch
            checked={refreshing}
            onChange={handleSwitchChange}
            name="refresh"
            inputProps={{ "aria-label": "secondary checkbox" }}
          />
          <br />
          Request Status: {msg} <br />
        </TitleCard>
      </div>
    );
  }

  const slsLogs = slsDisplayer(jobInfo.slsUrl || {});

  return (
    <div className={classes.root}>
      <PageTitle title={`${jobInfo.name || jobInfo.jobId} - JOB`} />
      <TitleCard
        title={
          <div>
            JOB - {params.id} <StatusChip type="job" status={jobInfo.state} />
            <Tooltip title="Auto Refresh" placement="right">
              <Switch
                size="small"
                checked={refreshing}
                onChange={handleSwitchChange}
                name="refresh"
                inputProps={{ "aria-label": "secondary checkbox" }}
              />
            </Tooltip>
          </div>
        }
      >
        <JobPipeline job_id={jobInfo.jobId} />
      </TitleCard>
      <TitleCard title="Detail">
        <Tabs
          value={selectedTab}
          onChange={handleChange}
          className={classes.tab}
        >
          <Tab value="info" label="Info" />
          <Tab value="resource" label="Resource" />
          <Tab value="dep" label="Dependencies" />
          <Tab
            value="worker"
            label={`Worker(${job?.jobWorkers?.filter(({ coreWorkerStats }) => coreWorkerStats?.[0]?.currentTaskName).length || 0})`}
          />
          <Tab
            value="actor"
            label={`Actor(${Object.entries(job?.jobActors || {}).length || 0})`}
          />
          <Tab value="event" label="Events" />
        </Tabs>
        {selectedTab === "info" && (
          <Grid container spacing={2}>
            <Grid item xs={12}>
              <Grid container spacing={2}>
                {slsLogs.map((e) => (
                  <Grid item>
                    <Button>{e}</Button>
                  </Grid>
                ))}
                {jobInfo.eventUrl && (
                  <Grid item>
                    <Button>
                      <a
                        href={jobInfo.eventUrl}
                        target="_blank"
                        rel="noopener noreferrer"
                      >
                        Event(SLS)
                      </a>
                    </Button>
                  </Grid>
                )}
                {ipLogMap[jobInfo.driverIpAddress] && (
                  <Grid item>
                    <Button>
                      <Link
                        to={`/log/${encodeURIComponent(
                          ipLogMap[jobInfo.driverIpAddress],
                        )}?fileName=driver-${jobInfo?.config?.metadata?.job_submission_id || jobInfo.jobId}`}
                        target="_blank"
                      >
                        Driver Log
                      </Link>
                    </Button>
                  </Grid>
                )}
                {jobInfo.metricUrl && (
                  <Grid item>
                    <Button>
                      <a
                        href={jobInfo.metricUrl}
                        target="_blank"
                        rel="noopener noreferrer"
                      >
                        Metric Url
                      </a>
                    </Button>
                  </Grid>
                )}
                <Grid item>
                  <Button>
                    <a
                      href={jobInfo.url}
                      target="_blank"
                      rel="noopener noreferrer"
                    >
                      Job Package
                    </a>
                  </Button>
                </Grid>
                <Grid item>
                  <Button>
                    <Link
                      target="_blank"
                      to={`/cmd/${jobInfo.language === "JAVA" ? "jstack" : "pystack"
                        }/${jobInfo.driverIpAddress}/${jobInfo.driverPid}`}
                    >
                      Driver {jobInfo.language === "JAVA" && "Jstack"}
                      {jobInfo.language === "PYTHON" && "Pystack"}
                    </Link>
                  </Button>
                </Grid>
              </Grid>
            </Grid>
            <Grid item xs={4}>
              <span className={classes.label}>Name</span>: {jobInfo.name}
            </Grid>
            <Grid item xs={4}>
              <span className={classes.label}>Owner</span>: {jobInfo.owner}
            </Grid>
            <Grid item xs={4}>
              <span className={classes.label}>Language</span>:{" "}
              {jobInfo.language}
            </Grid>
            <Grid item xs={4}>
              <span className={classes.label}>Namespace</span>:{" "}
              {jobInfo.namespaceId}
            </Grid>
            <Grid item xs={4}>
              <span className={classes.label}>Driver Entry</span>:{" "}
              {jobInfo.driverEntry}
            </Grid>
            <Grid item xs={4}>
              <span className={classes.label}>Driver IP</span>:{" "}
              {jobInfo.driverIpAddress}
            </Grid>
            <Grid item xs={4}>
              <span className={classes.label}>Driver HostName</span>:{" "}
              {jobInfo.driverHostname}
            </Grid>
            <Grid item xs={4}>
              <span className={classes.label}>Driver Pid</span>:{" "}
              {jobInfo.driverPid}
            </Grid>
            {jobInfo.failErrorMessage && (
              <Grid item xs={12}>
                <span className={classes.label}>Fail Error</span>:{" "}
                <pre className={classes.alert} style={{ whiteSpace: "pre-line", wordBreak: 'break-all' }}>{jobInfo.failErrorMessage}</pre>
              </Grid>
            )}
            <Grid item xs={12}>
              <span className={classes.label}>JVM Options</span>:{" "}
              <pre style={{ whiteSpace: "pre-line" }}>{jobInfo.jvmOptions}</pre>
            </Grid>
            <Grid item xs={12}>
              <span className={classes.label}>Driver Args</span>:{" "}
              {jsonFormat(jobInfo.driverArgs)}
            </Grid>
            <Grid item xs={12}>
              <span className={classes.label}>Driver Cmd</span>:{" "}
              <pre style={{ whiteSpace: "pre-line", wordBreak: "break-all" }}>
                {jobInfo.driverCmdline}
              </pre>
            </Grid>
            <Grid item xs={12}>
              <h4>User Custom Config</h4>
              <TableContainer className={classes.paper}>
                <Table>
                  <TableHead>
                    <TableRow>
                      {["Key", "Value"].map((col) => (
                        <TableCell align="center" key={col}>
                          {col}
                        </TableCell>
                      ))}
                    </TableRow>
                  </TableHead>
                  <TableBody>
                    {Object.entries(jobInfo.customConfig || {}).map(
                      ([k, v]) => (
                        <TableRow key={k}>
                          <TableCell align="center">{k}</TableCell>
                          <TableCell style={{ maxWidth: 500 }} align="center">
                            {jsonFormat(v)}
                          </TableCell>
                        </TableRow>
                      ),
                    )}
                  </TableBody>
                </Table>
              </TableContainer>
            </Grid>
          </Grid>
        )}
        {selectedTab === "resource" && (
          <JobResouce jobId={params.id} jobInfo={jobInfo} />
        )}
        {jobInfo?.dependencies && selectedTab === "dep" && (
          <div className={classes.paper}>
            {jobInfo?.dependencies?.python && (
              <TitleCard title="Python Dependencies">
                <div
                  style={{
                    display: "flex",
                    justifyItems: "space-around",
                    flexWrap: "wrap",
                  }}
                >
                  {jobInfo.dependencies.python.map((e) => (
                    <StatusChip
                      type="deps"
                      status={e.startsWith("http") ? longTextCut(e, 30) : e}
                      key={e}
                    />
                  ))}
                </div>
              </TitleCard>
            )}
            {jobInfo?.dependencies?.java && (
              <TitleCard title="Java Dependencies">
                <TableContainer>
                  <Table>
                    <TableHead>
                      <TableRow>
                        {["Name", "Version", "URL"].map((col) => (
                          <TableCell align="center" key={col}>
                            {col}
                          </TableCell>
                        ))}
                      </TableRow>
                    </TableHead>
                    <TableBody>
                      {jobInfo.dependencies.java.map(
                        ({ name, version, url }) => (
                          <TableRow key={url}>
                            <TableCell align="center">{name}</TableCell>
                            <TableCell align="center">{version}</TableCell>
                            <TableCell align="center">
                              <a
                                href={url}
                                target="_blank"
                                rel="noopener noreferrer"
                              >
                                {url}
                              </a>
                            </TableCell>
                          </TableRow>
                        ),
                      )}
                    </TableBody>
                  </Table>
                </TableContainer>
              </TitleCard>
            )}
          </div>
        )}

        {selectedTab === "worker" && (
          <div>
            <TableContainer className={classes.paper}>
              <RayletWorkerTable
                workers={job.jobWorkers.filter((({ coreWorkerStats }) => coreWorkerStats?.[0]?.currentTaskName))}
                actorMap={actorMap || {}}
                entity="job"
                entityId={jobInfo.jobId}
              />
            </TableContainer>
          </div>
        )}
        {selectedTab === "actor" && (
          <div>
            <TableContainer className={classes.paper}>
              <ActorTable actors={actorMap || {}} workers={job.jobWorkers} />
            </TableContainer>
          </div>
        )}
        {selectedTab === "event" && (
          <EventTable slsUrl={jobInfo?.eventUrl} job_id={jobInfo.jobId} />
        )}
        {selectedTab === "result" && <JobResult jobId={jobInfo.jobId} />}
      </TitleCard>
    </div>
  );
};

export default JobDetailPage;
