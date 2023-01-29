import { Color } from "@material-ui/core";
import {
  blue,
  blueGrey,
  cyan,
  green,
  grey,
  lightBlue,
  orange,
  red,
} from "@material-ui/core/colors";
import { CSSProperties } from "@material-ui/core/styles/withStyles";
import React, { ReactNode } from "react";
import { ActorEnum } from "../type/actor";

const colorMap = {
  node: {
    ALIVE: green,
    DEAD: red,
  },
  actor: {
    [ActorEnum.ALIVE]: green,
    [ActorEnum.DEAD]: red,
    [ActorEnum.PENDING]: blue,
    [ActorEnum.RECONSTRUCTING]: lightBlue,
  },
  job: {
    INIT: grey,
    SUBMITTED: blue,
    DISPATCHED: lightBlue,
    RUNNING: green,
    COMPLETED: cyan,
    FINISHED: cyan,
    FAILED: red,
  },
  deathCause: {
    NORMAL: green,
    "JOB DIED": orange,
    "OWNER DIED": orange,
    "RAY_KILLED": orange,
    "WORKER DIED": red,
    "NODE DIED": red,
    "CREATION EXCEPTION": red,
    "RUNTIME ENV":red,
    "BUNDLE REMOVED":red,
    "PG REMOVED":red,
    "SCHEDULING FAILED":red
  },
} as {
  [key: string]: {
    [key: string]: Color;
  };
};

const typeMap = {
  deps: blue,
  INFO: cyan,
  ERROR: red,
  WARNING: orange,
} as {
  [key: string]: Color;
};

export const StatusChip = ({
  type,
  status,
  suffix,
}: {
  type: string;
  status: string | ActorEnum | ReactNode;
  suffix?: string;
}) => {
  const style = {
    padding: "2px 8px",
    border: "solid 1px",
    borderRadius: 4,
    fontSize: 12,
    margin: 2,
    cursor: "default",
  } as CSSProperties;

  let color = blueGrey as Color;

  if (typeMap[type]) {
    color = typeMap[type];
  } else if (
    typeof status === "string" &&
    colorMap[type] &&
    colorMap[type][status]
  ) {
    color = colorMap[type][status];
  }

  style.color = color[500];
  style.borderColor = color[500];
  if (color !== blueGrey) {
    style.backgroundColor = `${color[500]}20`;
  }

  return (
    <span style={style}>
      {status}
      {suffix}
    </span>
  );
};
