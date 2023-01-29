import { Grid, IconButton, Tooltip } from "@material-ui/core";
import {
  ChevronRight,
  CloudOff,
  CloudQueue,
  Computer,
  ExpandMoreOutlined,
} from "@material-ui/icons";
import { get } from "lodash";
import React, { ReactElement, useContext, useEffect, useState } from "react";
import { Link } from "react-router-dom";
import { GlobalContext } from "../../../App";
import CopyableCollapse from "../../../components/CopyableCollapse";
import PercentageBar from "../../../components/PercentageBar";
import TitleCard from "../../../components/TitleCard";
import { getNamespacesLayeredResource } from "../../../service/resource";
import {
  HostResourceView,
  NamespaceResourceView,
  ResourceUnit,
} from "../../../type/resource";

const formatNum = (num: any) => {
  const number = Number(num);
  return Number.isInteger(number) ? number.toFixed(0) : number.toFixed(2);
};

const showResources = (resources?: ResourceUnit[]) =>
  resources?.length
    ? resources?.map((rs) => {
      const { available = 0, total = 0 } = rs;
      const used = total - available;
      const unit = rs.resourceName === 'memory' ? ' MB' : '';

      return (
        <Grid item>
          <PercentageBar num={used} total={total}>
            {rs.resourceName}:{formatNum(used)}{unit}/{formatNum(total)}{unit}(
            {formatNum((used / total) * 100)}%)
          </PercentageBar>
        </Grid>
      );
    })
    : null;

const HostResourcesLayer = ({
  views,
  icon,
  title,
  resources,
  type,
}: {
  views: HostResourceView[];
  icon: ReactElement;
  title?: string;
  resources?: ResourceUnit[];
  type: string;
}) => {
  const [collapsed, setCollapsed] = useState(title ? true : false);
  const { nodeMap } = useContext(GlobalContext);

  return (
    <div
      style={{
        borderLeft: "rgba(128, 128, 128, 0.5) solid 2px",
        width: "100%",
      }}
    >
      {title && (
        <Grid container spacing={3} alignItems="center">
          <Grid item>
            <IconButton size="small" onClick={() => setCollapsed(!collapsed)}>
              {collapsed ? <ChevronRight /> : <ExpandMoreOutlined />}
            </IconButton>
          </Grid>
          <Grid item>
            <Tooltip title={`This is a ${type}`}>{icon}</Tooltip>
          </Grid>
          <Grid item>{title}</Grid>
          {showResources(resources)}
        </Grid>
      )}
      {!collapsed && (
        <Grid
          container
          direction="row"
          spacing={2}
          style={{
            paddingLeft: 12,
          }}
        >
          {views?.map((view) => (
            <Grid item>
              <Grid container spacing={3} alignItems="center">
                <Grid item>
                  <Tooltip title={`This is a node`}>
                    <Computer />
                  </Tooltip>
                </Grid>
                <Grid item>
                  <Link target="_blank" to={`/node/${nodeMap[view.hostName]}`}>
                    {view.hostName}
                  </Link>
                </Grid>
                {showResources(view.resources)}
              </Grid>
            </Grid>
          ))}
        </Grid>
      )}
    </div>
  );
};

const NamespsaceResourceLayer = ({ view }: { view: NamespaceResourceView }) => {
  const [collapsed, setCollapsed] = useState(true);

  return (
    <div
      style={{
        borderLeft: "rgba(128, 128, 128, 0.5) solid 2px",
        width: "100%",
      }}
    >
      <Grid container spacing={3} alignItems="center">
        <Grid item>
          <IconButton size="small" onClick={() => setCollapsed(!collapsed)}>
            {collapsed ? <ChevronRight /> : <ExpandMoreOutlined />}
          </IconButton>
        </Grid>
        <Grid item>
          <Tooltip title="This is a namespace">
            <CloudQueue />
          </Tooltip>
        </Grid>
        <Grid item>
          <CopyableCollapse
            text={view.namespaceId || get(view, "namepaceId")}
            noCollapse
          />
        </Grid>
        {showResources(view.resources)}
      </Grid>
      {!collapsed && (
        <Grid
          container
          direction="row"
          spacing={2}
          style={{
            paddingLeft: 16,
          }}
        >
          {view.namespaces?.map((e) => (
            <Grid item>
              <NamespsaceResourceLayer view={e} />
            </Grid>
          ))}
          {view.hosts?.length > 0 && (
            <Grid item>
              <HostResourcesLayer
                type="Node"
                icon={<Computer />}
                views={view.hosts}
              />
            </Grid>
          )}
          {view.unassignedHosts?.hosts?.length > 0 && (
            <Grid item>
              <HostResourcesLayer
                type="Unassigned Host"
                icon={<CloudOff />}
                title="Unassigned Host"
                views={view.unassignedHosts.hosts}
                resources={view.unassignedHosts.resources}
              />
            </Grid>
          )}
        </Grid>
      )}
    </div>
  );
};

const LayeredNSResourceCard = () => {
  const [resourceView, setView] = useState<NamespaceResourceView>();

  useEffect(() => {
    getNamespacesLayeredResource().then((res) => {
      if (res.data.data.resourceView) {
        setView(res.data.data.resourceView);
      }
    });
  }, []);

  return (
    <TitleCard title="Namespace">
      {resourceView && <NamespsaceResourceLayer view={resourceView} />}
    </TitleCard>
  );
};

export default LayeredNSResourceCard;
