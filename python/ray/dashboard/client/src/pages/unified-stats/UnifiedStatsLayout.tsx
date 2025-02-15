import { Box } from "@mui/material";
import React from "react";
import { Outlet } from "react-router-dom";
import { MainNavPageInfo } from "../layout/mainNavContext";
import { SystemHealthCard } from "./components/SystemHealthCard";
import { ResourceUtilizationCard } from "./components/ResourceUtilizationCard";
import { ActorDashboardCard } from "./components/ActorDashboardCard";
import { JobsMonitoringCard } from "./components/JobsMonitoringCard";
import { NodesOverviewCard } from "./components/NodesOverviewCard";
import { AdvancedAnalyticsCard } from "./components/AdvancedAnalyticsCard";

export const UnifiedStatsLayout = () => {
  return (
    <Box sx={{ padding: 3 }}>
      <MainNavPageInfo
        pageInfo={{
          id: "unified-stats",
          title: "Unified Stats",
          path: "/unified-stats",
        }}
      />
      
      <Box sx={{ display: "grid", gridTemplateColumns: "repeat(2, 1fr)", gap: 3 }}>
        {/* System Health Overview */}
        <SystemHealthCard />
        
        {/* Resource Utilization */}
        <ResourceUtilizationCard />
        
        {/* Actor Dashboard */}
        <ActorDashboardCard />
        
        {/* Jobs Monitoring */}
        <JobsMonitoringCard />
        
        {/* Nodes Overview */}
        <NodesOverviewCard />
        
        {/* Advanced Analytics */}
        <AdvancedAnalyticsCard />
      </Box>
      
      <Outlet />
    </Box>
  );
}; 