import React, { useEffect, useState } from "react";
import ResourceUnitPie from "../../../components/ResourceUnitPie";
import TitleCard from "../../../components/TitleCard";
import { getClusterResource } from "../../../service/resource";
import { ClusterResources, ResourceUnit } from "../../../type/resource";

export default () => {
  const [resources, setResource] = useState<ClusterResources>();
  const data = resources
    ? (["memory", "CPU"]
        .filter((e) => resources[e])
        .map((e) => [e, resources[e]]) as [string, ResourceUnit][])
    : [];

  useEffect(() => {
    getClusterResource().then((res) => setResource(res.data.data.resources));
  }, []);

  if (!data.length) {
    return null;
  }

  return (
    <TitleCard title="Cluster Resource">
      <div
        style={{
          display: "flex",
          flexFlow: "row wrap",
          justifyContent: "space-around",
        }}
      >
        {data.map(([name, resource]) => (
          <div style={{ margin: 5, textAlign: "center", flex: 1 }}>
            <ResourceUnitPie resource={resource} title={name} />
          </div>
        ))}
      </div>
    </TitleCard>
  );
};
