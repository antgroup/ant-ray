import React from "react";
import { Box, Card, CardContent, Typography } from "@mui/material";
import { useClusterMetrics } from "../../../hooks/useClusterMetrics";
import {
  LineChart,
  Line,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  Legend,
  ResponsiveContainer,
} from "recharts";

interface ChartDataPoint {
  timestamp: number;
  cpu: number;
  memory: number;
  gpu?: number;
}

export const ResourceUtilizationCard = () => {
  const { data: metrics, error } = useClusterMetrics();

  if (error) {
    return (
      <Card>
        <CardContent>
          <Typography color="error">
            Error loading resource metrics: {error.toString()}
          </Typography>
        </CardContent>
      </Card>
    );
  }

  if (!metrics) {
    return (
      <Card>
        <CardContent>
          <Typography>Loading resource metrics...</Typography>
        </CardContent>
      </Card>
    );
  }

  // Transform metrics data for the chart
  const chartData: ChartDataPoint[] = metrics.timestamps.map(
    (timestamp: number, index: number) => ({
      timestamp,
      cpu: metrics.cpuUsage[index] * 100,
      memory: metrics.memoryUsage[index] * 100,
      ...(metrics.gpuUsage ? { gpu: metrics.gpuUsage[index] * 100 } : {}),
    })
  );

  return (
    <Card>
      <CardContent>
        <Typography variant="h6" gutterBottom>
          Resource Utilization Trends
        </Typography>

        <Box sx={{ width: "100%", height: 300 }}>
          <ResponsiveContainer>
            <LineChart data={chartData}>
              <CartesianGrid strokeDasharray="3 3" />
              <XAxis
                dataKey="timestamp"
                tickFormatter={(timestamp: number) =>
                  new Date(timestamp).toLocaleTimeString()
                }
              />
              <YAxis
                tickFormatter={(value: number) => `${value.toFixed(1)}%`}
                domain={[0, 100]}
              />
              <Tooltip
                formatter={(value: number) => `${value.toFixed(1)}%`}
                labelFormatter={(timestamp: number) =>
                  new Date(timestamp).toLocaleString()
                }
              />
              <Legend />
              <Line
                type="monotone"
                dataKey="cpu"
                stroke="#2196F3"
                name="CPU Usage"
                dot={false}
              />
              <Line
                type="monotone"
                dataKey="memory"
                stroke="#4CAF50"
                name="Memory Usage"
                dot={false}
              />
              {metrics.gpuUsage && (
                <Line
                  type="monotone"
                  dataKey="gpu"
                  stroke="#F44336"
                  name="GPU Usage"
                  dot={false}
                />
              )}
            </LineChart>
          </ResponsiveContainer>
        </Box>

        <Box sx={{ mt: 2 }}>
          <Typography variant="subtitle1" gutterBottom>
            Current Usage
          </Typography>
          <Box sx={{ display: "flex", gap: 2 }}>
            <Box sx={{ flex: 1 }}>
              <Typography variant="body2" color="textSecondary">
                CPU
              </Typography>
              <Typography variant="h6">
                {(metrics.cpuUsage[metrics.cpuUsage.length - 1] * 100).toFixed(1)}
                %
              </Typography>
            </Box>
            <Box sx={{ flex: 1 }}>
              <Typography variant="body2" color="textSecondary">
                Memory
              </Typography>
              <Typography variant="h6">
                {(
                  metrics.memoryUsage[metrics.memoryUsage.length - 1] * 100
                ).toFixed(1)}
                %
              </Typography>
            </Box>
            {metrics.gpuUsage && (
              <Box sx={{ flex: 1 }}>
                <Typography variant="body2" color="textSecondary">
                  GPU
                </Typography>
                <Typography variant="h6">
                  {(
                    metrics.gpuUsage[metrics.gpuUsage.length - 1] * 100
                  ).toFixed(1)}
                  %
                </Typography>
              </Box>
            )}
          </Box>
        </Box>
      </CardContent>
    </Card>
  );
}; 