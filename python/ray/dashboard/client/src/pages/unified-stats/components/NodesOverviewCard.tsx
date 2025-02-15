import React from "react";
import { Box, Card, CardContent, Typography } from "@mui/material";
import {
  AreaChart,
  Area,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  ResponsiveContainer,
} from "recharts";
import { useNodeMetrics } from "../../../service/node";

export const NodesOverviewCard = () => {
  const { data: nodeMetrics, error } = useNodeMetrics();

  if (error) {
    return (
      <Card>
        <CardContent>
          <Typography color="error">
            Error loading node metrics: {error.toString()}
          </Typography>
        </CardContent>
      </Card>
    );
  }

  if (!nodeMetrics) {
    return (
      <Card>
        <CardContent>
          <Typography>Loading node metrics...</Typography>
        </CardContent>
      </Card>
    );
  }

  // Transform metrics data for the chart
  const chartData = nodeMetrics.timestamps.map((timestamp: number, index: number) => ({
    timestamp,
    cpuUsage: nodeMetrics.cpuUsage[index] * 100,
    memoryUsage: nodeMetrics.memoryUsage[index] * 100,
    networkReceived: nodeMetrics.networkReceived[index],
    networkSent: nodeMetrics.networkSent[index],
  }));

  return (
    <Card>
      <CardContent>
        <Typography variant="h6" gutterBottom>
          Nodes Overview
        </Typography>

        <Box sx={{ mb: 3 }}>
          <Typography variant="subtitle1" gutterBottom>
            Resource Usage Over Time
          </Typography>
          <Box sx={{ width: "100%", height: 200 }}>
            <ResponsiveContainer>
              <AreaChart data={chartData}>
                <CartesianGrid strokeDasharray="3 3" />
                <XAxis
                  dataKey="timestamp"
                  tickFormatter={(timestamp: number) =>
                    new Date(timestamp).toLocaleTimeString()
                  }
                />
                <YAxis
                  yAxisId="percentage"
                  tickFormatter={(value: number) => `${value.toFixed(1)}%`}
                  domain={[0, 100]}
                />
                <YAxis
                  yAxisId="network"
                  orientation="right"
                  tickFormatter={(value: number) =>
                    `${(value / 1024 / 1024).toFixed(1)} MB/s`
                  }
                />
                <Tooltip
                  formatter={(value: number, name: string) => {
                    if (name.includes("Usage")) {
                      return `${value.toFixed(1)}%`;
                    }
                    return `${(value / 1024 / 1024).toFixed(1)} MB/s`;
                  }}
                  labelFormatter={(timestamp: number) =>
                    new Date(timestamp).toLocaleString()
                  }
                />
                <Area
                  type="monotone"
                  dataKey="cpuUsage"
                  name="CPU Usage"
                  stroke="#2196F3"
                  fill="#2196F3"
                  fillOpacity={0.2}
                  yAxisId="percentage"
                />
                <Area
                  type="monotone"
                  dataKey="memoryUsage"
                  name="Memory Usage"
                  stroke="#4CAF50"
                  fill="#4CAF50"
                  fillOpacity={0.2}
                  yAxisId="percentage"
                />
                <Area
                  type="monotone"
                  dataKey="networkReceived"
                  name="Network Received"
                  stroke="#FFC107"
                  fill="#FFC107"
                  fillOpacity={0.2}
                  yAxisId="network"
                />
                <Area
                  type="monotone"
                  dataKey="networkSent"
                  name="Network Sent"
                  stroke="#FF5722"
                  fill="#FF5722"
                  fillOpacity={0.2}
                  yAxisId="network"
                />
              </AreaChart>
            </ResponsiveContainer>
          </Box>
        </Box>

        <Box sx={{ display: "flex", gap: 2 }}>
          <Box sx={{ flex: 1 }}>
            <Typography variant="subtitle2" color="textSecondary">
              Total Nodes
            </Typography>
            <Typography variant="h6">{nodeMetrics.totalNodes}</Typography>
          </Box>
          <Box sx={{ flex: 1 }}>
            <Typography variant="subtitle2" color="textSecondary">
              Active Nodes
            </Typography>
            <Typography variant="h6">{nodeMetrics.activeNodes}</Typography>
          </Box>
          <Box sx={{ flex: 1 }}>
            <Typography variant="subtitle2" color="textSecondary">
              Total CPU Cores
            </Typography>
            <Typography variant="h6">{nodeMetrics.totalCpuCores}</Typography>
          </Box>
          <Box sx={{ flex: 1 }}>
            <Typography variant="subtitle2" color="textSecondary">
              Total Memory
            </Typography>
            <Typography variant="h6">
              {(nodeMetrics.totalMemoryGB).toFixed(1)} GB
            </Typography>
          </Box>
        </Box>
      </CardContent>
    </Card>
  );
}; 