import React from "react";
import { Box, Card, CardContent, Typography } from "@mui/material";
import {
  ScatterChart,
  Scatter,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  ResponsiveContainer,
} from "recharts";
import {
  useAdvancedMetrics,
  MetricSample,
} from "../../../service/analytics";

interface CorrelationDataPoint {
  cpuUsage: number;
  taskThroughput: number;
}

export const AdvancedAnalyticsCard = () => {
  const { data: metrics, error } = useAdvancedMetrics();

  if (error) {
    return (
      <Card>
        <CardContent>
          <Typography color="error">
            Error loading advanced metrics: {error.toString()}
          </Typography>
        </CardContent>
      </Card>
    );
  }

  if (!metrics) {
    return (
      <Card>
        <CardContent>
          <Typography>Loading advanced metrics...</Typography>
        </CardContent>
      </Card>
    );
  }

  // Transform data for the correlation scatter plot
  const correlationData: CorrelationDataPoint[] = metrics.samples.map(
    (sample: MetricSample) => ({
      cpuUsage: sample.cpuUsage * 100,
      taskThroughput: sample.taskThroughput,
    })
  );

  return (
    <Card>
      <CardContent>
        <Typography variant="h6" gutterBottom>
          Advanced Analytics
        </Typography>

        <Box sx={{ mb: 3 }}>
          <Typography variant="subtitle1" gutterBottom>
            CPU Usage vs Task Throughput Correlation
          </Typography>
          <Box sx={{ width: "100%", height: 200 }}>
            <ResponsiveContainer>
              <ScatterChart>
                <CartesianGrid strokeDasharray="3 3" />
                <XAxis
                  dataKey="cpuUsage"
                  name="CPU Usage"
                  unit="%"
                  type="number"
                  domain={[0, 100]}
                />
                <YAxis
                  dataKey="taskThroughput"
                  name="Task Throughput"
                  unit=" tasks/s"
                />
                <Tooltip
                  formatter={(value: number, name: string) => {
                    if (name === "CPU Usage") {
                      return `${value.toFixed(1)}%`;
                    }
                    return `${value.toFixed(1)} tasks/s`;
                  }}
                />
                <Scatter
                  data={correlationData}
                  fill="#2196F3"
                  name="CPU-Throughput"
                />
              </ScatterChart>
            </ResponsiveContainer>
          </Box>
        </Box>

        <Box sx={{ display: "flex", gap: 2 }}>
          <Box sx={{ flex: 1 }}>
            <Typography variant="subtitle2" color="textSecondary">
              Average Task Latency
            </Typography>
            <Typography variant="h6">
              {metrics.averageTaskLatency.toFixed(2)} ms
            </Typography>
          </Box>
          <Box sx={{ flex: 1 }}>
            <Typography variant="subtitle2" color="textSecondary">
              Task Success Rate
            </Typography>
            <Typography variant="h6">
              {(metrics.taskSuccessRate * 100).toFixed(1)}%
            </Typography>
          </Box>
          <Box sx={{ flex: 1 }}>
            <Typography variant="subtitle2" color="textSecondary">
              Resource Efficiency
            </Typography>
            <Typography variant="h6">
              {(metrics.resourceEfficiency * 100).toFixed(1)}%
            </Typography>
          </Box>
        </Box>

        <Box sx={{ mt: 2 }}>
          <Typography variant="subtitle2" color="textSecondary" gutterBottom>
            System Insights
          </Typography>
          {metrics.insights.map((insight: string, index: number) => (
            <Typography
              key={index}
              variant="body2"
              color="textSecondary"
              sx={{ mb: 0.5 }}
            >
              • {insight}
            </Typography>
          ))}
        </Box>
      </CardContent>
    </Card>
  );
}; 