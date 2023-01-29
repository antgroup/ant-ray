import { Button, Input, makeStyles, Paper } from "@material-ui/core";
import React, { useState } from "react";
import { RouteComponentProps } from "react-router-dom";
import Loading from "../../components/Loading";
import PercentageBar from "../../components/PercentageBar";
import TitleCard from "../../components/TitleCard";
import { getLogProxy } from "../../service/log";
import { getFireGraph } from "../../service/util";
import { PageTitle } from "../../util/func";

const useStyles = makeStyles((theme) => ({
  root: {
    padding: theme.spacing(4),
    width: "100%",
  },
  table: {
    marginTop: theme.spacing(4),
    padding: theme.spacing(2),
  },
  pageMeta: {
    padding: theme.spacing(2),
    marginTop: theme.spacing(2),
  },
  search: {
    margin: theme.spacing(1),
  },
}));

const Profiler: React.FC<RouteComponentProps<{ pid: string; ip: string }>> = (
  props,
) => {
  const {
    match: {
      params: { pid, ip },
    },
    location: { pathname },
  } = props;
  const [img, setImg] = useState<string>();
  const [duration, setDuration] = useState(60);
  const [loading, setLoading] = useState(false);
  const [count, setCount] = useState(0);
  const [error, setError] = useState<string>();
  const classes = useStyles();
  const profiler =
    pathname.split("/")[1] === "profiler"
      ? "async-profiler"
      : pathname.split("/")[1];
  const getGraph = () => {
    setLoading(true);
    setError(undefined);
    getFireGraph(ip, duration, Number(pid), profiler)
      .then((res) => {
        if (res.data?.data?.output) {
          setCount(0);
          waitForSVGReady(res.data?.data?.output, 0);
        }
      })
      .catch(() => {
        setLoading(false);
      });
  };
  const waitForSVGReady = async (src: string, c: number) => {
    if (c < duration) {
      setCount(c + 1);
      setTimeout(() => {
        waitForSVGReady(src, c + 1);
      }, 1000);
    } else {
      try {
        const data = await getLogProxy(src);
        if (data.startsWith("<?xml")) {
          setLoading(false);
          setImg(src);
        } else if (data.includes("err")) {
          setError(`${data}`);
          setLoading(false);
        } else {
          setCount(c + 1);
          setTimeout(() => {
            waitForSVGReady(src, c + 1);
          }, 1000);
        }
      } catch (e) {
        setLoading(false);
        setImg(src);
      }
    }
  };

  return (
    <div className={classes.root}>
      <PageTitle title={`Profiler for ${ip} on Process ${pid}`} />
      <TitleCard title="Profiler">
        <div>
          IP: {ip}, Pid: {pid}, Duration:{" "}
          <Input
            style={{ width: 54 }}
            value={duration}
            renderSuffix={() => "s"}
            onChange={({ target: { value } }) => {
              setDuration(Number(value));
            }}
          />{" "}
        </div>
        <Button variant="contained" onClick={getGraph}>
          Run
        </Button>
        {error && <p>Error: {error}</p>}
      </TitleCard>
      {img && (
        <Paper>
          <Button
            target="_blank"
            href={`log_proxy?url=${encodeURIComponent(img)}`}
          >
            Open in new window
          </Button>
          <iframe
            title="fire-svg"
            style={{
              width: "100%",
              height: 600,
              border: "none",
              marginTop: 24,
            }}
            src={`log_proxy?url=${encodeURIComponent(img)}`}
            onError={(err) => console.log(err)}
          />
        </Paper>
      )}
      <Loading
        loading={loading}
        title={
          <PercentageBar total={duration} num={count}>
            Profiling {count || "waiting to start"}s ( Total {duration}s )
          </PercentageBar>
        }
      />
    </div>
  );
};

export default Profiler;
