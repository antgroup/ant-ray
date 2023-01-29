import {
  Accordion,
  AccordionDetails,
  AccordionSummary,
  Button,
  Grid,
  makeStyles,
  TableBody,
  TableCell,
  TableContainer,
  TableHead,
  TableRow,
} from "@material-ui/core";
import { ExpandMore } from "@material-ui/icons";
import React, { useEffect, useState } from "react";
import { Link } from "react-router-dom";
import CopyableCollapse from "../../components/CopyableCollapse";
import TitleCard from "../../components/TitleCard";
import { getRayConfig } from "../../service/cluster";
import { getNodeList } from "../../service/node";
import { getNamespaces } from "../../service/util";
import { RayConfig } from "../../type/config";
import { NodeDetail } from "../../type/node";
import { memoryConverter } from "../../util/converter";
import { PageTitle, slsDisplayer } from "../../util/func";
import ClusterResourceCard from "./components/ClusterResourceCard";
import LayeredNSResourceCard from "./components/LayeredNSResourceCard";

const useStyle = makeStyles((theme) => ({
  root: {
    padding: theme.spacing(2),
  },
  label: {
    fontWeight: "bold",
  },
}));

const getVal = (key: string, value: any) => {
  if (key === "containerMemory") {
    return memoryConverter(value * 1024 * 1024);
  }
  if (key === "slsUrl") {
    return slsDisplayer(value);
  }
  return JSON.stringify(value);
};

const useIndex = () => {
  const [rayConfig, setConfig] = useState<RayConfig>();
  const [namespaces, setNamespaces] =
    useState<{ namespaceId: string; hostNameList: string[] }[]>();
  const [nodes, setNodes] = useState<NodeDetail[]>([]);
  useEffect(() => {
    getRayConfig().then((res) => {
      if (res?.data?.data?.config) {
        setConfig(res.data.data.config);
      }
    });
  }, []);
  useEffect(() => {
    getNamespaces().then((res) => {
      if (res?.data?.data?.namespaces) {
        setNamespaces(res.data.data.namespaces);
      }
    });
  }, []);
  useEffect(() => {
    getNodeList().then((res) => {
      if (res?.data?.data?.summary) {
        setNodes(res.data.data.summary);
      }
    });
  }, []);

  return { rayConfig, namespaces, nodes };
};

const Index = () => {
  const { rayConfig, namespaces, nodes } = useIndex();
  const classes = useStyle();
  const slsLogs = slsDisplayer(rayConfig?.slsUrl || {});

  return (
    <div className={classes.root}>
      <PageTitle title={`${rayConfig?.clusterName || "Ray"} Summary `} />
      <TitleCard title={rayConfig?.clusterName || "SUMMARY"}>
        <p>Frontend Version: 1.8.10</p>
        <Grid container spacing={2}>
          {rayConfig?.imageUrl && (
            <Grid item>
              <Button>
                <a
                  href={rayConfig.imageUrl}
                  target="_blank"
                  rel="noopener noreferrer"
                >
                  Image Url
                </a>
              </Button>
            </Grid>
          )}
          {rayConfig?.sourceCodeLink && (
            <Grid item>
              <Button>
                <a
                  href={rayConfig.sourceCodeLink}
                  target="_blank"
                  rel="noopener noreferrer"
                >
                  Source Code
                </a>
              </Button>
            </Grid>
          )}
          {rayConfig?.metricUrl && (
            <Grid item>
              <Button>
                <a
                  href={rayConfig.metricUrl}
                  target="_blank"
                  rel="noopener noreferrer"
                >
                  Metric Url
                </a>
              </Button>
            </Grid>
          )}
          {rayConfig?.slsUrl &&
            (slsLogs instanceof Array ? (
              slsLogs.map((e) => (
                <Grid item>
                  <Button>{e}</Button>
                </Grid>
              ))
            ) : (
              <Grid item>
                <Button>{slsLogs}</Button>
              </Grid>
            ))}
          {rayConfig?.rayOperatorEventUrl && (
            <Grid item>
              <Button>
                <a
                  href={rayConfig.rayOperatorEventUrl}
                  target="_blank"
                  rel="noopener noreferrer"
                >
                  Deploy Event(SLS)
                </a>
              </Button>
            </Grid>
          )}
        </Grid>
      </TitleCard>
      <ClusterResourceCard />

      {<LayeredNSResourceCard /> ||
        (namespaces && (
          <TitleCard title="Namespaces">
            {namespaces.map((namespace) => (
              <Accordion>
                <AccordionSummary expandIcon={<ExpandMore />}>
                  <p className={classes.label}>
                    <CopyableCollapse text={namespace.namespaceId} noCollapse />
                    ({namespace.hostNameList.length} Nodes)
                  </p>
                </AccordionSummary>
                <AccordionDetails>
                  <Grid style={{ maxHeight: 300, overflow: "auto" }}>
                    {namespace.hostNameList
                      .sort((a, b) => a.length - b.length)
                      .map((host) => (
                        <Button size="small">
                          {host.includes("[unregistered]") ? (
                            host
                          ) : (
                            <Link
                              key="host"
                              style={{ margin: 4 }}
                              to={`node/${
                                nodes.find(
                                  (e) =>
                                    e.hostname === host &&
                                    e.raylet.state === "ALIVE",
                                )?.raylet.nodeId || ""
                              }`}
                            >
                              {host}
                            </Link>
                          )}
                        </Button>
                      ))}
                  </Grid>
                </AccordionDetails>
              </Accordion>
            ))}
          </TitleCard>
        ))}
      {rayConfig && (
        <TitleCard title="Config">
          <TableContainer>
            <TableHead>
              <TableCell>Key</TableCell>
              <TableCell>Value</TableCell>
            </TableHead>
            <TableBody>
              {Object.entries(rayConfig).map(([key, value]) => (
                <TableRow>
                  <TableCell className={classes.label}>{key}</TableCell>
                  <TableCell
                    style={{ wordBreak: "break-all", whiteSpace: "pre-wrap" }}
                  >
                    {getVal(key, value)}
                  </TableCell>
                </TableRow>
              ))}
            </TableBody>
          </TableContainer>
        </TitleCard>
      )}
    </div>
  );
};

export default Index;
