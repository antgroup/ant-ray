import { Backdrop, CircularProgress } from "@material-ui/core";
import React, { ReactNode } from "react";

const Loading = ({
  loading,
  title = "loading",
}: {
  loading: boolean;
  title?: ReactNode;
}) => (
  <Backdrop open={loading} style={{ zIndex: 100, flexDirection: "column" }}>
    <CircularProgress color="primary" />
    <p style={{ color: "rgba(255, 255, 255, 0.65)" }}>{title}</p>
  </Backdrop>
);

export default Loading;
