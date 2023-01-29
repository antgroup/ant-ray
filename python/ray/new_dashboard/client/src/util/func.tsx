import { Button, Menu, Tooltip } from "@material-ui/core";
import React, {
  CSSProperties,
  ReactNode,
  ReactNodeArray,
  useState,
} from "react";
import { Helmet } from "react-helmet";

export const longTextCut = (text: string = "", len: number = 28) => (
  <Tooltip title={text} interactive>
    <span>{text.length > len ? text.slice(0, len) + "..." : text}</span>
  </Tooltip>
);

export const slsDisplayer = (slsUrl: string | { [key: string]: string }) => {
  if (typeof slsUrl === "string") {
    return [
      <a href={slsUrl} target="_blank" rel="noopener noreferrer">
        SLS
      </a>,
    ];
  }

  return Object.entries(slsUrl || {}).map(([filename, link]) => (
    <a
      key={filename}
      style={{ marginRight: 8 }}
      href={link}
      target="_blank"
      rel="noopener noreferrer"
    >
      {filename}(SLS)
    </a>
  ));
};

export const jsonFormat = (str: string | object) => {
  const preStyle = {
    textAlign: "left",
    wordBreak: "break-all",
    whiteSpace: "pre-wrap",
  } as CSSProperties;
  if (typeof str === "object") {
    return <pre style={preStyle}>{JSON.stringify(str, null, 2)}</pre>;
  }
  try {
    const j = JSON.parse(str);
    if (typeof j !== "object") {
      return JSON.stringify(j);
    }
    return <pre style={preStyle}>{JSON.stringify(j, null, 2)}</pre>;
  } catch (e) {
    return str;
  }
};

export const PageTitle: React.FC<{ title: ReactNode }> = ({ title }) => (
  <Helmet>
    <title>{title} - Ray Dashboard</title>
  </Helmet>
);

export const ButtonMenuList: React.FC<{
  name: ReactNode;
  list: ReactNodeArray;
}> = ({ name, list }) => {
  const [anchorEl, setAnchorEl] = useState<null | HTMLElement>(null);

  const handleClick = (event: React.MouseEvent<HTMLButtonElement>) => {
    setAnchorEl(event.currentTarget);
  };

  const handleClose = () => {
    setAnchorEl(null);
  };

  return (
    <div>
      <Button
        aria-controls="simple-menu"
        aria-haspopup="true"
        onClick={handleClick}
      >
        {name}
      </Button>
      <Menu
        anchorEl={anchorEl}
        keepMounted
        open={Boolean(anchorEl)}
        onClose={handleClose}
      >
        {list}
      </Menu>
    </div>
  );
};
