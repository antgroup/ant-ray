import React from "react";
import { Box, Card, CardContent, Typography } from "@mui/material";
import { PieChart, Pie, Cell, Tooltip, ResponsiveContainer } from "recharts";
import { useActorGroups, ActorGroup } from "../../../service/actor";

const COLORS = {
  ALIVE: "#4CAF50",
  DEAD: "#F44336",
  RESTARTING: "#FFC107",
};

interface ActorState {
  name: string;
  value: number;
  color: string;
}

export const ActorDashboardCard = () => {
  const { data: actorGroups, error } = useActorGroups();

  if (error) {
    return (
      <Card>
        <CardContent>
          <Typography color="error">
            Error loading actor data: {error.toString()}
          </Typography>
        </CardContent>
      </Card>
    );
  }

  if (!actorGroups) {
    return (
      <Card>
        <CardContent>
          <Typography>Loading actor data...</Typography>
        </CardContent>
      </Card>
    );
  }

  // Calculate actor state distribution
  const actorStates: ActorState[] = [
    {
      name: "Alive",
      value: actorGroups.summary.alive || 0,
      color: COLORS.ALIVE,
    },
    {
      name: "Dead",
      value: actorGroups.summary.dead || 0,
      color: COLORS.DEAD,
    },
    {
      name: "Restarting",
      value: actorGroups.summary.restarting || 0,
      color: COLORS.RESTARTING,
    },
  ];

  // Get top actor groups by resource usage
  const topActorGroups = actorGroups.groups
    .sort((a: ActorGroup, b: ActorGroup) => b.cpuUsage - a.cpuUsage)
    .slice(0, 5);

  return (
    <Card>
      <CardContent>
        <Typography variant="h6" gutterBottom>
          Actor Dashboard
        </Typography>

        <Box sx={{ display: "flex", alignItems: "center", mb: 2 }}>
          <Box sx={{ flex: 1, height: 200 }}>
            <ResponsiveContainer width="100%" height="100%">
              <PieChart>
                <Pie
                  data={actorStates}
                  dataKey="value"
                  nameKey="name"
                  cx="50%"
                  cy="50%"
                  innerRadius={60}
                  outerRadius={80}
                  paddingAngle={2}
                >
                  {actorStates.map((entry, index) => (
                    <Cell key={`cell-${index}`} fill={entry.color} />
                  ))}
                </Pie>
                <Tooltip />
              </PieChart>
            </ResponsiveContainer>
          </Box>

          <Box sx={{ flex: 1, pl: 2 }}>
            <Typography variant="subtitle1" gutterBottom>
              Actor States
            </Typography>
            {actorStates.map((entry) => (
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
          Top Actor Groups by CPU Usage
        </Typography>
        <Box sx={{ mt: 1 }}>
          {topActorGroups.map((group: ActorGroup, index: number) => (
            <Box
              key={index}
              sx={{
                display: "flex",
                alignItems: "center",
                mb: 1,
              }}
            >
              <Typography
                variant="body2"
                sx={{ flex: 1, whiteSpace: "nowrap", overflow: "hidden" }}
              >
                {group.name}
              </Typography>
              <Box
                sx={{
                  flex: 1,
                  mx: 1,
                  height: 4,
                  backgroundColor: "#E0E0E0",
                  borderRadius: 2,
                }}
              >
                <Box
                  sx={{
                    width: `${group.cpuUsage * 100}%`,
                    height: "100%",
                    backgroundColor: "#2196F3",
                    borderRadius: 2,
                  }}
                />
              </Box>
              <Typography variant="body2" sx={{ minWidth: 60 }}>
                {(group.cpuUsage * 100).toFixed(1)}%
              </Typography>
            </Box>
          ))}
        </Box>
      </CardContent>
    </Card>
  );
}; 