import { Pie } from "@ant-design/charts";
import { useTheme } from "@material-ui/core";
import React from "react";
import { ResourceUnit } from "../type/resource";

const ResourceUnitPie = (props: { resource: ResourceUnit; title: string }) => {
  const theme = useTheme();
  const { resource, title } = props;
  return (
    <div style={{ textAlign: "center" }}>
      {" "}
      <Pie
        label={{
          autoRotate: false,
          type: "inner",
          content: ({ type, percent, value }) => {
            return `${type}\n${value}\n${(percent * 100).toFixed(2)}%`;
          },
          style: {
            fonrSize: 12,
            fill: theme.palette.text.primary,
            stroke: theme.palette.background.paper,
            textAlign: "center",
            lineWidth: 2,
          },
          layout: { type: "fixedOverlap" },
        }}
        color={[theme.palette.primary.light, theme.palette.secondary.main]}
        width={100}
        height={120}
        style={{ margin: "0 auto" }}
        legend={false}
        angleField="value"
        colorField="type"
        data={[
          {
            type: "avalable",
            value: resource.available,
          },
          {
            type: "used",
            value: resource.total - resource.available,
          },
        ]}
        pieStyle={{
          stroke: theme.palette.background.paper,
        }}
      />
      <h5>{title}</h5>
    </div>
  );
};

export default React.memo(ResourceUnitPie, (p, n) => {
  return JSON.stringify(p.resource) === JSON.stringify(n.resource);
});
