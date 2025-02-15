import React from "react";
import { Box, Card, CardContent, Typography } from "@mui/material";
import { useClusterInfo } from "../../../service/cluster";
import { PieChart, Pie, Cell, Tooltip, ResponsiveContainer } from "recharts";

const COLORS = {
  ALIVE: "#4CAF50",
  DEAD: "#F44336",
};

export const SystemHealthCard = () => {
  const { data: clusterInfo, error } = useClusterInfo();

  if (error) {
    return (
      <Card>
        <CardContent>
          <Typography color="error">
            Error loading cluster info: {error.toString()}
          </Typography>
        </CardContent>
      </Card>
    );
  }

  if (!clusterInfo) {
    return (
      <Card>
        <CardContent>
          <Typography>Loading cluster info...</Typography>
        </CardContent>
      </Card>
    );
  }

  const nodeData = [
    {
      name: "Alive",
      value: clusterInfo.aliveNodes || 0,
      color: COLORS.ALIVE,
    },
    {
      name: "Dead",
      value: clusterInfo.deadNodes || 0,
      color: COLORS.DEAD,
    },
  ];

  return (
    <Card>
      <CardContent>
        <Typography variant="h6" gutterBottom>
          System Health Overview
        </Typography>
        
        <Box sx={{ display: "flex", alignItems: "center", mb: 2 }}>
          <Box sx={{ flex: 1, height: 200 }}>
            <ResponsiveContainer width="100%" height="100%">
              <PieChart>
                <Pie
                  data={nodeData}
                  dataKey="value"
                  nameKey="name"
                  cx="50%"
                  cy="50%"
                  innerRadius={60}
                  outerRadius={80}
                  paddingAngle={2}
                >
                  {nodeData.map((entry, index) => (
                    <Cell key={`cell-${index}`} fill={entry.color} />
                  ))}
                </Pie>
                <Tooltip />
              </PieChart>
            </ResponsiveContainer>
          </Box>
          
          <Box sx={{ flex: 1, pl: 2 }}>
            <Typography variant="subtitle1" gutterBottom>
              Node Status
            </Typography>
            {nodeData.map((entry) => (
              <Box
                key={entry.name}
                sx={{
                  display: "flex",
                  alignItems: "center",
                  mb: 1,
                }}
              >
                <Box
                  sx={{
                    width: 12,
                    height: 12,
                    borderRadius: "50%",
                    backgroundColor: entry.color,
                    mr: 1,
                  }}
                />
                <Typography variant="body2">
                  {entry.name}: {entry.value}
                </Typography>
              </Box>
            ))}
          </Box>
        </Box>

        <Typography variant="subtitle1" gutterBottom>
          Resource Utilization
        </Typography>
        <Box sx={{ display: "flex", gap: 2 }}>
          <Box sx={{ flex: 1 }}>
            <Typography variant="body2" color="textSecondary">
              CPU Usage
            </Typography>
            <Typography variant="h6">
              {((clusterInfo.totalCpuUsage || 0) * 100).toFixed(1)}%
            </Typography>
          </Box>
          <Box sx={{ flex: 1 }}>
            <Typography variant="body2" color="textSecondary">
              Memory Usage
            </Typography>
            <Typography variant="h6">
              {((clusterInfo.totalMemoryUsage || 0) * 100).toFixed(1)}%
            </Typography>
          </Box>
          {clusterInfo.totalGpuUsage !== undefined && (
            <Box sx={{ flex: 1 }}>
              <Typography variant="body2" color="textSecondary">
                GPU Usage
              </Typography>
              <Typography variant="h6">
                {(clusterInfo.totalGpuUsage * 100).toFixed(1)}%
              </Typography>
            </Box>
          )}
        </Box>
      </CardContent>
    </Card>
  );
}; 