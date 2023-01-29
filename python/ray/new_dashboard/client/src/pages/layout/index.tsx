import {
  AppBar,
  Drawer,
  Hidden,
  IconButton,
  List,
  ListItem,
  ListItemIcon,
  ListItemText,
  Toolbar,
  Tooltip,
  Typography,
} from "@material-ui/core";
import { makeStyles } from "@material-ui/core/styles";
import {
  Ballot,
  Dashboard,
  DeveloperBoard,
  EventNote,
  Menu,
  NightsStay,
  Receipt,
  VerticalAlignTop,
  WbSunny,
} from "@material-ui/icons";
import classnames from "classnames";
import React, { PropsWithChildren, useState } from "react";
import { RouteComponentProps } from "react-router-dom";

import SpeedTools from "../../components/SpeedTools";
import Logo from "../../logo.svg";
import { PageTitle } from "../../util/func";

const drawerWidth = 200;

const useStyles = makeStyles((theme) => ({
  root: {
    display: "flex",
    "& a": {
      color: theme.palette.primary.main,
    },
    [theme.breakpoints.down("md")]: {
      paddingTop: 64,
    },
  },
  drawer: {
    width: drawerWidth,
    flexShrink: 0,
    background: theme.palette.background.paper,
    [theme.breakpoints.up("md")]: {
      width: drawerWidth,
      flexShrink: 0,
    },
  },
  drawerPaper: {
    width: drawerWidth,
    border: "none",
    background: theme.palette.background.paper,
    boxShadow: theme.shadows[1],
  },
  title: {
    padding: theme.spacing(2),
    textAlign: "center",
    lineHeight: "36px",
  },
  divider: {
    background: "rgba(255, 255, 255, .12)",
  },
  menuItem: {
    cursor: "pointer",
    "&:hover": {
      background: theme.palette.primary.main,
    },
  },
  menuIcon: {
    color: theme.palette.text.secondary,
  },
  selected: {
    background: `linear-gradient(45deg, ${theme.palette.primary.main} 30%, ${theme.palette.secondary.main} 90%)`,
  },
  child: {
    flex: 1,
  },
  appBar: {
    [theme.breakpoints.up("md")]: {
      width: `calc(100% - ${drawerWidth}px)`,
      marginLeft: drawerWidth,
    },
  },
  menuButton: {
    marginRight: theme.spacing(2),
    [theme.breakpoints.up("md")]: {
      display: "none",
    },
  },
  // necessary for content to be below app bar
  toolbar: theme.mixins.toolbar,
  content: {
    flexGrow: 1,
    padding: theme.spacing(3),
  },
}));

const BasicLayout = (
  props: PropsWithChildren<
    { setTheme: (theme: string) => void; theme: string } & RouteComponentProps
  >,
) => {
  const classes = useStyles();
  const { location, history, children, setTheme, theme } = props;
  const [mobileOpen, setMobileOpen] = useState(false);
  const handleDrawerToggle = () => {
    setMobileOpen(!mobileOpen);
  };

  const menuList = (
    <List>
      <ListItem
        button
        className={classnames(
          classes.menuItem,
          location.pathname === "/" && classes.selected,
        )}
        onClick={() => history.push("/")}
      >
        <ListItemIcon>
          <Dashboard className={classes.menuIcon} />
        </ListItemIcon>
        <ListItemText>SUMMARY</ListItemText>
      </ListItem>
      <ListItem
        button
        className={classnames(
          classes.menuItem,
          location.pathname.includes("node") && classes.selected,
        )}
        onClick={() => history.push("/node")}
      >
        <ListItemIcon>
          <DeveloperBoard className={classes.menuIcon} />
        </ListItemIcon>
        <ListItemText>NODES</ListItemText>
      </ListItem>
      <ListItem
        button
        className={classnames(
          classes.menuItem,
          location.pathname.includes("job") && classes.selected,
        )}
        onClick={() => history.push("/job")}
      >
        <ListItemIcon>
          <Ballot className={classes.menuIcon} />
        </ListItemIcon>

        <ListItemText>JOBS</ListItemText>
      </ListItem>
      <ListItem
        button
        className={classnames(
          classes.menuItem,
          location.pathname.includes("event") && classes.selected,
        )}
        onClick={() => history.push("/event")}
      >
        {" "}
        <ListItemIcon>
          <EventNote className={classes.menuIcon} />
        </ListItemIcon>
        <ListItemText>EVENTS</ListItemText>
      </ListItem>
      <ListItem
        button
        className={classnames(
          classes.menuItem,
          location.pathname.includes("log") && classes.selected,
        )}
        onClick={() => history.push("/log")}
      >
        <ListItemIcon>
          <Receipt className={classes.menuIcon} />
        </ListItemIcon>
        <ListItemText>LOGS</ListItemText>
      </ListItem>
      <ListItem>
        <IconButton
          color="primary"
          onClick={() => {
            window.scrollTo(0, 0);
          }}
        >
          <Tooltip title="Back To Top">
            <VerticalAlignTop />
          </Tooltip>
        </IconButton>
        <IconButton
          color="primary"
          onClick={() => {
            setTheme(theme === "dark" ? "light" : "dark");
          }}
        >
          <Tooltip title={`Theme - ${theme}`}>
            {theme === "dark" ? <NightsStay /> : <WbSunny />}
          </Tooltip>
        </IconButton>
      </ListItem>
      <SpeedTools />
    </List>
  );

  return (
    <div className={classes.root}>
      <PageTitle title="" />
      <Hidden lgUp implementation="css">
        <AppBar color="inherit">
          <Toolbar>
            <IconButton onClick={handleDrawerToggle}>
              <Menu />
            </IconButton>
            <Typography variant="h6" style={{ flexGrow: 1, marginLeft: 16 }}>
              RayDashboard
            </Typography>
          </Toolbar>
        </AppBar>
        <Drawer
          anchor="left"
          classes={{
            paper: classes.drawerPaper,
          }}
          open={mobileOpen}
          onClose={handleDrawerToggle}
        >
          <Typography variant="h6" className={classes.title}>
            <img width={48} src={Logo} alt="Ray" /> <br /> Ray Dashboard
          </Typography>
          {menuList}
        </Drawer>
      </Hidden>
      <Hidden mdDown implementation="css">
        <Drawer
          variant="permanent"
          anchor="left"
          className={classes.drawer}
          classes={{
            paper: classes.drawerPaper,
          }}
        >
          <Typography variant="h6" className={classes.title}>
            <img width={48} src={Logo} alt="Ray" /> <br /> Ray Dashboard
          </Typography>
          {menuList}
        </Drawer>
      </Hidden>
      <div className={classes.child}>{children}</div>
    </div>
  );
};

export default BasicLayout;
