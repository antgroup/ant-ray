import axios from "axios";
import { NodeDetailRsp, NodeListRsp } from "../type/node";

export const getNodeList = async (includeInactive?: boolean) => {
  const url = `nodes?view=summary${includeInactive ? '' : '&exclude-inactive=true'}`

  return await axios.get<NodeListRsp>(url);
};

export const getNodeDetail = async (id: string) => {
  return await axios.get<NodeDetailRsp>(`nodes/${id}`);
};
