import {
  Button,
  InputAdornment,
  LinearProgress,
  List,
  ListItem,
  ListItemIcon,
  ListItemText,
  makeStyles,
  Paper,
  Switch,
  TextField,
} from "@material-ui/core";
import {
  ErrorOutline,
  Folder,
  HelpOutline,
  InsertDriveFile,
  Language,
  SearchOutlined,
} from "@material-ui/icons";
import React, { useEffect, useRef, useState } from "react";
import { RouteComponentProps } from "react-router-dom";
import LogVirtualView from "../../components/LogView/LogVirtualView";
import { SearchInput } from "../../components/SearchComponent";
import TitleCard from "../../components/TitleCard";
import { getLogDetail } from "../../service/log";
import { PageTitle } from "../../util/func";
import { brpcLinkChanger } from "../node";

const useStyles = makeStyles((theme) => ({
  root: {
    padding: theme.spacing(2),
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

type LogsProps = RouteComponentProps<{ host?: string; path?: string }> & {
  theme?: "dark" | "light";
};

const useLogs = (props: LogsProps) => {
  const {
    match: { params },
    location: { search: urlSearch },
    theme,
  } = props;
  const { host, path } = params;
  const searchMap = new URLSearchParams(urlSearch);
  const urlFileName = searchMap.get("fileName");
  const el = useRef<HTMLDivElement>(null);
  const [origin, setOrigin] = useState<string>();
  const [search, setSearch] = useState<{
    keywords?: string;
    lineNumber?: string;
    fontSize?: number;
    revert?: boolean;
  }>();
  const [fileName, setFileName] = useState(searchMap.get("fileName") || "");
  const [log, setLogs] = useState<
    undefined | string | { [key: string]: string }[]
  >();
  const [startTime, setStart] = useState<string>();
  const [endTime, setEnd] = useState<string>();

  useEffect(() => {
    setFileName(urlFileName || "");
  }, [urlFileName]);

  useEffect(() => {
    let url = "log_index";
    setLogs("Loading...");
    setStart("");
    setEnd("");
    setSearch(undefined);
    setFileName(urlFileName || "");
    if (host) {
      url = decodeURIComponent(host);
      setOrigin(new URL(url).origin);
      if (path) {
        url += decodeURIComponent(path);
      }
    } else {
      setOrigin(undefined);
    }
    getLogDetail(url)
      .then((res) => {
        if (res) {
          setLogs(res);
        } else {
          setLogs("(null)");
        }
      })
      .catch(() => {
        setLogs("Failed to load");
      });
  }, [host, path, urlFileName]);

  return {
    log,
    origin,
    host,
    path,
    el,
    search,
    setSearch,
    theme,
    fileName,
    setFileName,
    startTime,
    setStart,
    endTime,
    setEnd,
  };
};

const Logs = (props: LogsProps) => {
  const classes = useStyles();
  const {
    log,
    origin,
    path,
    el,
    search,
    setSearch,
    theme,
    fileName,
    setFileName,
    startTime,
    setStart,
    endTime,
    setEnd,
  } = useLogs(props);
  let href = "#/log/";

  if (origin) {
    if (path) {
      const after = decodeURIComponent(path).split("/");
      after.pop();
      if (after.length > 1) {
        href += encodeURIComponent(origin);
        href += "/";
        href += encodeURIComponent(after.join("/"));
      }
    }
  }

  return (
    <div className={classes.root} ref={el}>
      <TitleCard title="Logs Viewer">
        <PageTitle
          title={
            path
              ? `${decodeURIComponent(path || "")} on ${origin}`
              : `${origin || "Index"} Logs`
          }
        />
        <Paper>
          {!origin && <p>Please choose an url to get log path</p>}
          {origin && (
            <p>
              Now Path: {origin}
              {decodeURIComponent(path || "")}{" "}
              <Button
                target="_blank"
                href={brpcLinkChanger(
                  `${origin}${decodeURIComponent(path || "")}`,
                )}
              >
                Access Url
              </Button>
            </p>
          )}
          {origin && (
            <div>
              <Button
                variant="contained"
                href={href}
                className={classes.search}
              >
                Back To ../
              </Button>
              {typeof log === "object" && (
                <SearchInput
                  defaultValue={fileName}
                  label="File Name"
                  onChange={(val) => {
                    setFileName(val);
                  }}
                />
              )}
            </div>
          )}
        </Paper>
        <Paper>
          {typeof log === "object" && (
            <List>
              {log.filter((e) => !fileName || e?.name?.includes(fileName))
                .length ? (
                log
                  .filter((e) => !fileName || e?.name?.includes(fileName))
                  .sort((a) => (a.name?.endsWith("/") ? -1 : 1))
                  .map((e: { [key: string]: string }) => (
                    <ListItem key={e.name}>
                      <ListItemIcon>
                        {e.name?.startsWith("http") && <Language />}
                        {!e.name?.startsWith("http") &&
                          (e.name?.endsWith("/") ? (
                            <Folder />
                          ) : (
                            <InsertDriveFile />
                          ))}
                      </ListItemIcon>
                      <ListItemText>
                        <a
                          href={`#/log/${
                            origin ? `${encodeURIComponent(origin)}/` : ""
                          }${encodeURIComponent(e.href)}`}
                        >
                          {e.name}
                        </a>
                      </ListItemText>
                    </ListItem>
                  ))
              ) : (
                <ListItem>
                  <ListItemIcon>
                    <HelpOutline />
                  </ListItemIcon>
                  <ListItemText>
                    There's no log found. Better try another one.
                  </ListItemText>
                </ListItem>
              )}
            </List>
          )}
          {log === "(null)" && (
            <List>
              <ListItem>
                <ListItemIcon>
                  <HelpOutline />
                </ListItemIcon>
                <ListItemText>
                  There's no log found in this path. Better try another one.
                </ListItemText>
              </ListItem>
            </List>
          )}
          {log === "Failed to load" && (
            <List>
              <ListItem>
                <ListItemIcon>
                  <ErrorOutline />
                </ListItemIcon>
                <ListItemText>
                  Logs port on this host not available.
                </ListItemText>
              </ListItem>
            </List>
          )}
          {typeof log === "string" &&
            !["Failed to load", "Loading...", "(null)"].includes(log) && (
              <div>
                <div>
                  <TextField
                    className={classes.search}
                    label="Keyword"
                    value={search?.keywords}
                    InputProps={{
                      onChange: ({ target: { value } }) => {
                        setSearch({ ...search, keywords: value });
                      },
                      type: "",
                      endAdornment: (
                        <InputAdornment position="end">
                          <SearchOutlined />
                        </InputAdornment>
                      ),
                    }}
                  />
                  <TextField
                    className={classes.search}
                    label="Line Number"
                    value={search?.lineNumber}
                    InputProps={{
                      onChange: ({ target: { value } }) => {
                        setSearch({ ...search, lineNumber: value });
                      },
                      type: "",
                      endAdornment: (
                        <InputAdornment position="end">
                          <SearchOutlined />
                        </InputAdornment>
                      ),
                    }}
                  />
                  <TextField
                    className={classes.search}
                    value={search?.fontSize}
                    label="Font Size"
                    InputProps={{
                      onChange: ({ target: { value } }) => {
                        setSearch({ ...search, fontSize: Number(value) });
                      },
                      type: "",
                    }}
                  />
                  <TextField
                    id="datetime-local"
                    label="Start Time"
                    type="datetime-local"
                    value={startTime}
                    className={classes.search}
                    onChange={(val) => {
                      setStart(val.target.value);
                    }}
                    InputLabelProps={{
                      shrink: true,
                    }}
                  />
                  <TextField
                    label="End Time"
                    type="datetime-local"
                    value={endTime}
                    className={classes.search}
                    onChange={(val) => {
                      setEnd(val.target.value);
                    }}
                    InputLabelProps={{
                      shrink: true,
                    }}
                  />
                  <div className={classes.search}>
                    Reverse:{" "}
                    <Switch
                      checked={search?.revert}
                      onChange={(e, v) => setSearch({ ...search, revert: v })}
                    />
                    <Button
                      className={classes.search}
                      variant="contained"
                      onClick={() => {
                        setStart("");
                        setEnd("");
                      }}
                    >
                      Reset Time
                    </Button>
                  </div>
                </div>
                <LogVirtualView
                  height={600}
                  theme={theme}
                  revert={search?.revert}
                  keywords={search?.keywords}
                  focusLine={Number(search?.lineNumber) || undefined}
                  fontSize={search?.fontSize || 12}
                  content={log}
                  language="prolog"
                  startTime={startTime}
                  endTime={endTime}
                  handleClickLineNumber={(line) => {
                    setSearch({
                      revert: search?.revert,
                      lineNumber: String(line),
                      keywords: "",
                    });
                  }}
                />
              </div>
            )}
          {log === "Loading..." && (
            <div>
              <br />
              <LinearProgress />
            </div>
          )}
        </Paper>
      </TitleCard>
    </div>
  );
};

export default Logs;
