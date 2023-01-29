import { CssBaseline } from "@material-ui/core";
import { ThemeProvider } from "@material-ui/core/styles";
import React, { Suspense, useEffect, useState } from "react";
import { Provider } from "react-redux";
import { HashRouter, Route, Switch } from "react-router-dom";
import Profiler from "./pages/cmd/Profiler";
import Loading from "./pages/exception/Loading";
import { getNodeList } from "./service/node";
import { getNamespaces } from "./service/util";
import { store } from "./store";
import { darkTheme, lightTheme } from "./theme";
import { getLocalStorage, setLocalStorage } from "./util/localData";

// lazy loading fro prevent loading too much code at once
const Actors = React.lazy(() => import("./pages/actor"));
const CMDResult = React.lazy(() => import("./pages/cmd/CMDResult"));
const Events = React.lazy(() => import("./pages/event/Events"));
const Index = React.lazy(() => import("./pages/index/Index"));
const Job = React.lazy(() => import("./pages/job"));
const JobDetail = React.lazy(() => import("./pages/job/JobDetail"));
const BasicLayout = React.lazy(() => import("./pages/layout"));
const Logs = React.lazy(() => import("./pages/log/Logs"));
const Node = React.lazy(() => import("./pages/node"));
const NodeDetail = React.lazy(() => import("./pages/node/NodeDetail"));

// key to store theme in local storage
const RAY_DASHBOARD_THEME_KEY = "ray-dashboard-theme";

// a global map for relations
export const GlobalContext = React.createContext({
  nodeMap: {} as { [key: string]: string },
  ipLogMap: {} as { [key: string]: string },
  namespaceMap: {} as { [key: string]: string[] },
  nodeIpMap: {} as { [key: string]: string },
});

export const getDefaultTheme = () =>
  getLocalStorage<string>(RAY_DASHBOARD_THEME_KEY) || "light";
export const setLocalTheme = (theme: string) =>
  setLocalStorage(RAY_DASHBOARD_THEME_KEY, theme);

const App = () => {
  const [theme, _setTheme] = useState(getDefaultTheme());
  const [context, setContext] = useState<{
    nodeMap: { [key: string]: string };
    nodeIpMap: { [key: string]: string };
    ipLogMap: { [key: string]: string };
    namespaceMap: { [key: string]: string[] };
  }>({ nodeMap: {}, ipLogMap: {}, namespaceMap: {}, nodeIpMap: {} });
  const getTheme = (name: string) => {
    switch (name) {
      case "dark":
        return darkTheme;
      case "light":
      default:
        return lightTheme;
    }
  };
  const setTheme = (name: string) => {
    setLocalTheme(name);
    _setTheme(name);
  };

  useEffect(() => {
    getNodeList().then((res) => {
      if (res?.data?.data?.summary) {
        const nodeMap = {} as { [key: string]: string };
        const ipLogMap = {} as { [key: string]: string };
        const nodeIpMap = {} as { [key: string]: string };
        res.data.data.summary.forEach(({ hostname, raylet, ip, logUrl }) => {
          if (raylet.nodeId) {
            nodeMap[hostname] = raylet.nodeId;
          }
          nodeIpMap[hostname] = ip;
          if (logUrl) {
            ipLogMap[ip] = logUrl;
          }
        });
        getNamespaces()
          .then((res) => {
            const namespaceMap = {} as { [key: string]: string[] };
            if (res?.data?.data?.namespaces) {
              res.data.data.namespaces.forEach((namespace) => {
                const { namespaceId, hostNameList } = namespace;
                hostNameList.forEach((hostname) => {
                  if (!namespaceMap[hostname]) {
                    namespaceMap[hostname] = [];
                  }
                  namespaceMap[hostname].push(namespaceId);
                });
              });
            }
            setContext({ nodeMap, ipLogMap, namespaceMap, nodeIpMap });
          })
          .catch(() => {
            setContext({ nodeMap, ipLogMap, namespaceMap: {}, nodeIpMap: {} });
          });
      }
    });
  }, []);

  return (
    <ThemeProvider theme={getTheme(theme)}>
      <Suspense fallback={Loading}>
        <GlobalContext.Provider value={context}>
          <Provider store={store}>
            <CssBaseline />
            <HashRouter>
              <Switch>
                <Route
                  render={(props) => (
                    <BasicLayout {...props} setTheme={setTheme} theme={theme}>
                      <Route component={Index} exact path="/" />
                      <Route component={Job} exact path="/job" />
                      <Route component={Node} exact path="/node" />
                      <Route component={Events} exact path="/event" />
                      <Route component={Actors} exact path="/actors" />
                      <Route
                        render={(props) => (
                          <Logs {...props} theme={theme as "light" | "dark"} />
                        )}
                        exact
                        path="/log/:host?/:path?"
                      />
                      <Route component={NodeDetail} path="/node/:id" />
                      <Route component={JobDetail} path="/job/:id" />
                      <Route component={CMDResult} path="/cmd/:cmd/:ip/:pid" />
                      <Route component={Profiler} path="/profiler/:ip/:pid" />
                      <Route component={Profiler} path="/pyprofiler/:ip/:pid" />
                      <Route component={Loading} exact path="/loading" />
                    </BasicLayout>
                  )}
                  path="/"
                />
              </Switch>
            </HashRouter>
          </Provider>
        </GlobalContext.Provider>
      </Suspense>
    </ThemeProvider>
  );
};

export default App;
