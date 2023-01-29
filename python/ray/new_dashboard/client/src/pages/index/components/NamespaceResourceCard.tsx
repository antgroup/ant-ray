import {
  Button,
  Input,
  Table,
  TableBody,
  TableCell,
  TableHead,
  TablePagination,
  TableRow,
} from "@material-ui/core";
import { ArrowDownwardOutlined, SearchOutlined } from "@material-ui/icons";
import { get } from "lodash";
import React, { useContext, useEffect, useState } from "react";
import { Link } from "react-router-dom";
import { GlobalContext } from "../../../App";
import CopyableCollapse from "../../../components/CopyableCollapse";
import PercentageBar from "../../../components/PercentageBar";
import TitleCard from "../../../components/TitleCard";
import { ExpandableTableRow } from "../../../components/WorkerTable";
import { getNamespacesResource } from "../../../service/resource";
import { ResourceUnit } from "../../../type/resource";

const getUsed = (key: string, obj: any) =>
  get(obj, ["nodeResource", key, "total"], 0) -
  get(obj, ["nodeResource", key, "available"], 0);

const sortByKey = (key: string) => (a: any, b: any) =>
  key ? getUsed(b, key) - getUsed(a, key) : 0;

const SubNodeResourcesTable = ({
  hostResources,
  cols,
}: {
  hostResources: {
    hostName: string;
    nodeResource: {
      [key: string]: ResourceUnit;
    };
  }[];
  cols: string[];
}) => {
  const [searchHost, setSearchHost] = useState("");
  const [sortKey, setSortkey] = useState("");
  const [pageNo, setPageNo] = useState(0);
  const [pageSize, setPageSize] = useState(10);
  const { nodeMap } = useContext(GlobalContext);
  const data = hostResources
    .filter((e) => !searchHost || e.hostName.includes(searchHost))
    .sort(sortByKey(sortKey));
  const pageData = data.slice(pageNo * pageSize, (pageNo + 1) * pageSize);

  useEffect(() => {
    setPageNo(0);
  }, [searchHost, pageSize]);

  return (
    <Table size="small">
      <TableHead>
        <TableCell>
          Hostname{" "}
          <Input
            value={searchHost}
            onChange={({ target: { value } }) => setSearchHost(value)}
            endAdornment={<SearchOutlined />}
          />
        </TableCell>
        {cols.map((col) => (
          <TableCell>
            {col}{" "}
            <Button
              onClick={() => setSortkey(sortKey === col ? "" : col)}
              size="small"
            >
              {sortKey === col ? (
                <ArrowDownwardOutlined color="primary" />
              ) : (
                <ArrowDownwardOutlined />
              )}
            </Button>
          </TableCell>
        ))}
      </TableHead>
      <TableBody>
        {pageData.map((hres) => (
          <TableRow>
            <TableCell>
              <Link target="_blank" to={`/node/${nodeMap[hres.hostName]}`}>
                {hres.hostName}
              </Link>
            </TableCell>
            {cols.map((col) => {
              const { total, available } = hres.nodeResource[col] || {};
              const used = total - available;
              if (total === undefined || available === undefined) {
                return null;
              }
              return (
                <TableCell>
                  <PercentageBar num={used} total={total}>
                    {used?.toFixed(0)}/{total?.toFixed(0)}(
                    {Math.round((used / total) * 100)?.toFixed(0)}%)
                  </PercentageBar>
                </TableCell>
              );
            })}
          </TableRow>
        ))}
      </TableBody>
      <TablePagination
        count={data.length}
        rowsPerPage={pageSize}
        onChangeRowsPerPage={({ target: { value } }) => {
          setPageSize(Number(value));
        }}
        onPageChange={(evt, page) => setPageNo(page)}
        page={pageNo}
      />
    </Table>
  );
};

const NamespaceResourceCard = () => {
  const [namespaceRes, setNSRes] = useState<
    {
      namespaceId: string;
      hostResources: {
        hostName: string;
        nodeResource: {
          [key: string]: ResourceUnit;
        };
      }[];
      resourcesMap: {
        [key: string]: ResourceUnit;
      };
    }[]
  >([]);
  const [cols, setCols] = useState<string[]>([]);
  useEffect(() => {
    getNamespacesResource().then((res) => {
      if (res?.data?.data?.resourceView) {
        const result = res.data.data.resourceView;
        const colSet = new Set<string>();
        const resources = result.map((rv) => {
          const { namespaceId } = rv;

          const resourcesMap: { [key: string]: ResourceUnit } = {};
          const hostResources = rv.hostResources.map((hr) => {
            const nodeResource: { [key: string]: ResourceUnit } = {};
            hr.resources.forEach((runit) => {
              if (runit.resourceName) {
                colSet.add(runit.resourceName);
                nodeResource[runit.resourceName] = {
                  available: runit.available,
                  total: runit.total,
                };
                if (!resourcesMap[runit.resourceName]) {
                  resourcesMap[runit.resourceName] = {
                    available: runit.available,
                    total: runit.total,
                  };
                } else {
                  resourcesMap[runit.resourceName].available += runit.available;
                  resourcesMap[runit.resourceName].total += runit.total;
                }
              }
            });
            return { hostName: hr.hostName, nodeResource };
          });
          return {
            namespaceId,
            hostResources,
            resourcesMap,
          };
        });
        setNSRes(resources);
        setCols(Array.from(colSet));
      }
    });
  }, []);

  if (!namespaceRes.length) {
    return null;
  }

  return (
    <TitleCard title="Namespaces">
      <Table>
        <TableHead>
          <TableCell></TableCell>
          <TableCell align="center">Namespaces Id</TableCell>
          {cols.map((col) => (
            <TableCell align="center">{col}</TableCell>
          ))}
        </TableHead>
        <TableBody>
          {namespaceRes.map((nsres) => (
            <ExpandableTableRow
              length={nsres.hostResources.length}
              expandComponent={
                <SubNodeResourcesTable
                  cols={cols}
                  hostResources={nsres.hostResources}
                />
              }
            >
              <TableCell align="center">
                {" "}
                <CopyableCollapse text={nsres.namespaceId} noCollapse />
              </TableCell>
              {cols.map((col) => {
                const { total = 0, available = 0 } =
                  nsres.resourcesMap[col] || {};
                const used = total - available;
                return (
                  <TableCell align="center">
                    <PercentageBar num={used} total={total}>
                      {used?.toFixed(0)}/{total?.toFixed(0)}(
                      {Math.round((used / total) * 100)?.toFixed(0)}%)
                    </PercentageBar>
                  </TableCell>
                );
              })}
            </ExpandableTableRow>
          ))}
        </TableBody>
      </Table>
    </TitleCard>
  );
};

export default NamespaceResourceCard;
