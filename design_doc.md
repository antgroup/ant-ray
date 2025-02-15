# Ray Unified Stats Dashboard PRD

## 1. Product Overview
### 1.1 Purpose
Create a centralized statistics visualization dashboard that provides comprehensive insights into Ray cluster operations through modern data visualizations and real-time metrics aggregation.

### 1.2 Target Users
- Cluster Administrators
- Ray Application Developers
- System Reliability Engineers
- Data Engineering Teams

## 2. Core Features

### 2.1 Global Cluster Summary
- **System Health Overview Card**
  - Node status distribution (Active/Dead)
    - Interactive donut chart with drill-down capability
    - Hover tooltips showing detailed node metadata
    - Click-through to detailed node view
  - Resource utilization heatmap (CPU, GPU, Memory)
    - Multi-level treemap showing hierarchical resource allocation
    - Color gradients indicating utilization levels (0-100%)
    - Separate views for different resource types
  - Object store memory breakdown
    - Stacked area chart showing memory evolution over time
    - Memory categorization (used/available/spilled)
    - Configurable warning thresholds
  - Active tasks/actors timeline
    - Gantt-style visualization with parallel task streams
    - Color-coded by task type and priority
    - Zoom and pan controls for temporal navigation

```typescript:src/pages/overview/OverviewPage.tsx
startLine: 53
endLine: 56
```

### 2.2 Resource-Specific Dashboards
#### Actors Dashboard
- State distribution pie chart (ALIVE/DEAD/RESTARTING)
  - Animated transitions between state changes
  - Nested rings showing actor hierarchies
  - Filtering by actor namespace and runtime
- Creation timeline with lifecycle events
  - Swimlane diagram showing actor lifecycle
  - Event markers for important state transitions
  - Correlation lines between related actors
- Top 10 actor classes by resource usage
  - Bar chart race animation for dynamic updates
  - Multiple metric options (CPU/Memory/Custom)
  - Expandable to show full distribution
- Detailed actor table with sorting/filtering
  - Real-time metric updates with delta indicators
  - Inline sparklines for trend visualization
  - Customizable column sets for different use cases

```typescript:src/components/ActorTable.tsx
startLine: 535
endLine: 726
```

#### Jobs Monitoring
- Job duration distribution histogram
  - Log-scale option for wide-ranging durations
  - Kernel density estimation overlay
  - Percentile markers and outlier highlighting
- Status timeline (Pending/Running/Stopped)
  - Multi-series area chart with stacking
  - Interactive legend with series toggling
  - Time-window selection for detailed analysis
- Resource allocation sunburst chart
  - Hierarchical view of resource distribution
  - Dynamic resizing based on allocation changes
  - Click-to-zoom for detailed resource inspection
- Job dependency visualization
  - Force-directed graph with physics simulation
  - Edge weights based on data transfer volume
  - Collapsible job groups for large graphs

#### Nodes Overview
- Geographical distribution map (if available)
  - Cluster-aware node grouping
  - Health status indicators
  - Resource availability overlays
- Resource utilization radial gauges
  - Multi-metric circular displays
  - Threshold markers with alerts
  - Historical high/low indicators
- Node failure rate trends
  - Moving average with confidence intervals
  - Anomaly detection highlighting
  - Predictive trend lines
- Detailed hardware metrics table
  - Real-time hardware telemetry
  - System-level performance indicators
  - Resource pressure scoring

#### Workers Dashboard
- Worker state distribution visualization
  - Interactive stacked bar chart showing worker states
  - Categorization by worker type (IO/CPU/GPU)
  - Real-time updates with state transitions
- Worker lifetime timeline
  - Gantt chart showing worker lifespans
  - Color coding by worker status
  - Resource utilization indicators
- Worker-to-node mapping
  - Sankey diagram showing worker distribution
  - Node capacity utilization
  - Worker migration patterns

#### Tasks Dashboard
- Task execution heatmap
  - Time-based distribution of task execution
  - Color intensity by resource consumption
  - Filtering by task type and priority
- Task dependency graph
  - DAG visualization of task relationships
  - Critical path highlighting
  - Bottleneck identification
- Task scheduling metrics
  - Queue depth visualization
  - Scheduling latency distribution
  - Resource wait time analysis

#### Objects Dashboard
- Object store memory usage
  - Multi-level pie chart by object size
  - Age-based coloring for garbage collection
  - Spilled objects tracking
- Object reference graph
  - Force-directed graph of object dependencies
  - Edge weights based on access patterns
  - Object lifetime visualization
- Object transfer metrics
  - Network bandwidth utilization
  - Transfer latency distribution
  - Copy vs move operation analysis

#### Placement Groups Dashboard
- Placement strategy visualization
  - Bundle layout optimization view
  - Resource constraints representation
  - Scheduling efficiency metrics
- Group status timeline
  - State transition visualization
  - Failure recovery tracking
  - Resource reservation patterns
- Cross-node placement analysis
  - Node affinity visualization
  - Resource fragmentation metrics
  - Placement success rate trends

#### Runtime Environments Dashboard
- Environment creation timeline
  - Build time distribution
  - Cache hit rate visualization
  - Resource overhead tracking
- Dependency graph
  - Package relationship visualization
  - Version compatibility matrix
  - Size and complexity metrics
- Usage statistics
  - Environment adoption heatmap
  - Resource impact analysis
  - Initialization time trends

#### Virtual Clusters Dashboard
- Resource allocation sunburst
  - Hierarchical resource distribution
  - Dynamic quota visualization
  - Utilization efficiency metrics
- Cross-cluster communication
  - Inter-cluster traffic heatmap
  - Resource sharing patterns
  - Isolation boundary visualization
- Performance comparison
  - Cluster efficiency radar chart
  - Resource utilization comparison
  - Scheduling fairness metrics

#### Cluster Events Timeline
- Event stream visualization
  - Chronological event waterfall
  - Severity-based categorization
  - Correlation pattern detection
- Impact analysis
  - Resource state changes
  - Performance impact heatmap
  - Recovery time tracking
- Event filtering and search
  - Custom time window selection
  - Event type categorization
  - Pattern-based filtering

### 2.3 Advanced Analytics
- **Cross-Resource Correlation**
  - Task-actor dependency graph
  - Object store memory vs task execution heatmap
  - Placement group efficiency metrics

- **Time-Series Analysis**
  - Customizable time range selector
  - Metric comparison overlays
  - Anomaly detection alerts

## 3. Visualization Components

### 3.1 Summary Cards
- Compact metric cards with sparklines
- Status indicators with color-coded thresholds
- Quick comparison widgets (vs previous period)

```typescript:src/components/StatusChip.tsx
startLine: 18
endLine: 63
```

### 3.2 Interactive Charts
- Dynamic D3.js force-directed graphs for resource relationships
  - Physics-based node positioning
  - Dynamic edge bundling for clarity
  - Zoom/pan with semantic zooming
  - Node clustering by attribute
- Zoomable time-series line charts
  - Brush and zoom controls
  - Multiple Y-axis support
  - Annotation capabilities
  - Moving averages and trends
- Drill-down capable hierarchical visualizations
  - Collapsible tree layouts
  - Sunburst diagrams
  - Treemaps with size and color encoding
  - Voronoi treemaps for dense data
- GPU utilization heat matrices
  - Real-time GPU core activity
  - Memory bandwidth visualization
  - Temperature and power monitoring
  - Multi-GPU correlation views
- Advanced visualization types
  - Parallel coordinates for multi-dimensional analysis
  - Sankey diagrams for resource flow
  - Chord diagrams for inter-node communication
  - Matrix plots for correlation analysis
  - Bubble charts for multi-metric comparison
  - Stream graphs for temporal patterns
  - Radar charts for metric comparisons
  - Box plots for distribution analysis

### 3.3 Data Tables
- Paginated tables with multi-column sorting
- Context-aware quick filters
- Column presets for common views
- CSV/JSON export functionality

```python:src/ray/util/state/api.py
startLine: 237
endLine: 286
```

## 4. Technical Requirements

### 4.1 Data Handling
- Real-time updates via WebSocket (5s refresh)
- Client-side data caching with TTL
- Smart sampling for large datasets (>10k entries)
- Batch API requests for correlated metrics

### 4.2 Visualization Stack
- React-based component library
- Recharts for standard charts
- Three.js for 3D cluster visualizations
- Vis.js for network graphs

### 4.3 Performance
- Virtualized scrolling for large tables
- WebWorker-based data processing
- Canvas-based rendering for dense visualizations
- Lazy-loaded visualization modules

## 5. User Experience

### 5.1 Layout Principles
- Responsive grid layout with draggable/resizable widgets
- Preset dashboard templates
- Fullscreen mode for focused analysis
- Cross-filtering between visualizations

### 5.2 Interaction Design
- Hover tooltips with statistical summaries
- Click-to-drill contextual menus
- Annotation system for markups
- Shared dashboard links with preserved state

### 5.3 Accessibility
- WCAG 2.1 AA compliance
- Keyboard navigation support
- Screen reader optimized tables
- High-contrast visualization themes

## 6. API Integration

### 6.2 Metrics Processing
- Server-side aggregation for summary stats
- Streaming data compression
- Query optimization for time-range filters
- Field masking for performance

## 7. Security & Governance

- Role-based access control
- Sensitive data redaction
- Audit logging for data access
- Query rate limiting

## 8. Success Metrics

- 30% reduction in CLI usage for cluster inspection
- 25% faster debugging cycle times
- 90%+ satisfaction in usability surveys
- <500ms response time for core interactions

## 9. Open Questions

- How to handle multi-cluster aggregation?
- Optimal default time window for new users?
- Custom metric registration workflow?
- Embedded notebook integration strategy?

## 10. Mockups
[Placeholder for interactive prototype link]

## 11. References

```doc/source/ray-observability/reference/system-metrics.rst
startLine: 1
endLine: 93
```

```doc/source/ray-observability/user-guides/cli-sdk.rst
startLine: 324
endLine: 360
```
