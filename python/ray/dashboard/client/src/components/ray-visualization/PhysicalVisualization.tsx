import * as d3 from "d3";
import { Selection, ZoomBehavior } from "d3";
import React, { forwardRef, useCallback, useEffect, useRef, useState } from "react";
import { FormControl, InputLabel, Select, MenuItem } from "@mui/material";
import "./RayVisualization.css";
import { PhysicalViewData } from "../../service/physical-view";

// Utility function to generate color from actor ID
const getColorFromId = (id: string) => {
  // Simple hash function
  let hash = 0;
  for (let i = 0; i < id.length; i++) {
    hash = ((hash << 5) - hash) + id.charCodeAt(i);
    hash = hash & hash;
  }
  
  // Generate HSL color with good saturation and lightness
  const hue = Math.abs(hash) % 360;
  return `hsl(${hue}, 70%, 75%)`;
};

// Create a color scale for resource usage
const getResourceUsageColor = (usage: number) => {
  // Create color scale from green (low usage) to red (high usage)
  const colorScale = d3.scaleLinear<string>()
    .domain([0, 0.5, 0.8, 1])  // Usage thresholds
    .range(['#00e676', '#ffeb3b', '#ff9100', '#ff1744'])  // More vibrant colors
    .interpolate(d3.interpolateRgb);  // Use RGB interpolation for more vibrant transitions
  
  return colorScale(usage);
};

// Type for resource values
type ResourceValue = {
  total: number;
  available: number;
};

// Utility function to extract resource usage from node data
const extractResourceUsage = (resources: Record<string, ResourceValue>, pgId: string, resourceType: string) => {
  // Convert pgId to lowercase for case-insensitive matching
  const pgIdLower = pgId.toLowerCase();
  
  // Find the matching resource key
  const matchingKey = Object.keys(resources).find(key => {
    if (!key.includes('Group')) return false;
    const [type, id] = key.split('Group');
    return type.toLowerCase() === resourceType.toLowerCase() && 
           id.toLowerCase() === pgIdLower;
  });

  if (matchingKey) {
    const resourceValue = resources[matchingKey];
    return {
      available: resourceValue.available,
      total: resourceValue.total,
      used: resourceValue.total - resourceValue.available,
      usage: (resourceValue.total - resourceValue.available) / resourceValue.total
    };
  }
  return null;
};

// Get available resources for dropdown
const getAvailableResources = (resources: Record<string, ResourceValue>) => {
  const resourceMap = new Map<string, string>();
  
  Object.keys(resources)
    .filter(key => key.includes('Group') && key.split('Group')[0].toLowerCase() !== 'bundle')
    .forEach(key => {
      const [resourceType] = key.split('Group');
      if (!resourceMap.has(resourceType)) {
        resourceMap.set(resourceType, resourceType);
      }
    });

  // Sort resources for consistent display
  return Array.from(resourceMap.entries())
    .sort(([a], [b]) => a.localeCompare(b))
    .map(([key, value]) => ({
      key: value,
      label: value
    }));
};

// Add new type for context info
type ContextInfo = {
  key: string;
  value: any;
  actor_id: string;
};

// Utility function to get unique context keys
const getAvailableContextKeys = (physicalViewData: PhysicalViewData) => {
  const contextKeys = new Set<string>();
  contextKeys.add("actor_id"); // Always add actor_id as an option
  
  if (physicalViewData?.physicalView) {
    Object.values(physicalViewData.physicalView).forEach(nodeData => {
      if (nodeData?.actors) {
        Object.values(nodeData.actors).forEach((actor: any) => {
          if (actor?.contextInfo) {
            Object.keys(actor.contextInfo).forEach(key => contextKeys.add(key));
          }
        });
      }
    });
  }

  return Array.from(contextKeys).sort().map(key => ({
    key,
    label: key === "actor_id" ? "Actor ID" : key
  }));
};

// Utility function to get all unique context values for a key
const getUniqueContextValues = (physicalViewData: PhysicalViewData, contextKey: string) => {
  const values = new Set<string>();
  
  if (physicalViewData?.physicalView) {
    Object.values(physicalViewData.physicalView).forEach(nodeData => {
      if (nodeData?.actors) {
        Object.values(nodeData.actors).forEach((actor: any) => {
          if (contextKey === "actor_id") {
            if (actor?.actorId) {
              values.add(actor.actorId);
            }
          } else if (actor?.contextInfo?.[contextKey] !== undefined) {
            values.add(actor.contextInfo[contextKey].toString());
          }
        });
      }
    });
  }

  return Array.from(values);
};

// Create color scale for context values
const getContextColorScale = (values: string[]) => {
  const colorScale = d3.scaleOrdinal<string>()
    .domain(values)
    .range(d3.schemeCategory10);
  return colorScale;
};

type PhysicalVisualizationProps = {
  physicalViewData: PhysicalViewData;
  onElementClick: (data: any, skip_zoom: boolean) => void;
  selectedElementId: string | null;
  jobId?: string;
  updateKey?: number;
  onUpdate?: () => void;
  updating?: boolean;
};

const PhysicalVisualization = forwardRef<HTMLDivElement, PhysicalVisualizationProps>(
  (
    {
      physicalViewData,
      onElementClick,
      selectedElementId,
      jobId,
      updateKey,
      onUpdate,
      updating = false,
    },
    ref,
  ) => {
    const svgRef = useRef<SVGSVGElement | null>(null);
    const zoomRef = useRef<d3.ZoomBehavior<SVGSVGElement, unknown> | null>(null);
    const graphRef = useRef<d3.Selection<SVGGElement, unknown, null, any> | null>(null);
    const [selectedResource, setSelectedResource] = useState<string>("");
    const [selectedContext, setSelectedContext] = useState<string>("actor_id");
    const legendRef = useRef<SVGGElement | null>(null);

    // Colors for different elements
    const colors = {
      node: "#e8f5e9",
      nodeStroke: "#2e7d32",
      placementGroup: "#bbdefb",
      placementGroupStroke: "#1976d2",
      freeActors: "#ffecb3",
      freeActorsStroke: "#ff8f00",
      actor: "#c5e1a5",
      actorStroke: "#558b2f",
      selectedElement: "#ff5722",
      actorSection: "#ffffff",
      actorSectionStroke: "#90caf9",
    };

    const renderLegend = useCallback((contextKey: string) => {
      if (!svgRef.current || !physicalViewData) return;

      // Remove existing legend
      if (legendRef.current) {
        d3.select(legendRef.current).remove();
      }

      const svg = d3.select(svgRef.current);
      const values = getUniqueContextValues(physicalViewData, contextKey);
      const colorScale = getContextColorScale(values);

      const legendGroup = svg.append("g")
        .attr("class", "legend");
      
      legendRef.current = legendGroup.node() as SVGGElement;

      const legendWidth = 200;
      const legendItemHeight = 20;
      const legendX = 20; // Left side position
      const legendY = 50; // Increased from 20 to 50 to move lower
      const maxItems = Math.floor((parseInt(svg.style("height")) - 70) / legendItemHeight); // Adjusted for new top margin
      
      // Calculate number of columns needed
      const numColumns = Math.ceil(values.length / maxItems);
      const itemsPerColumn = Math.ceil(values.length / numColumns);

      // Draw legend background with increased width for longer text and multiple columns
      legendGroup.append("rect")
        .attr("x", legendX - 10)
        .attr("y", legendY - 25) // Adjusted to maintain proper spacing from title
        .attr("width", legendWidth * numColumns + 20)
        .attr("height", Math.min(values.length, itemsPerColumn) * legendItemHeight + 30) // Increased padding
        .attr("fill", "white")
        .attr("stroke", "#ccc")
        .attr("rx", 5)
        .attr("ry", 5);

      // Draw legend title
      legendGroup.append("text")
        .attr("x", legendX)
        .attr("y", legendY - 8) // Adjusted to maintain proper spacing
        .attr("font-size", "12px")
        .attr("font-weight", "bold")
        .text(contextKey === "actor_id" ? "Actor ID" : contextKey);

      // Draw legend items in columns
      values.forEach((value, index) => {
        const columnIndex = Math.floor(index / itemsPerColumn);
        const rowIndex = index % itemsPerColumn;
        
        const itemGroup = legendGroup.append("g")
          .attr("transform", `translate(${legendX + (columnIndex * legendWidth)}, ${legendY + (rowIndex * legendItemHeight)})`);

        // Color box
        itemGroup.append("rect")
          .attr("width", 15)
          .attr("height", 15)
          .attr("fill", colorScale(value))
          .attr("stroke", "#ccc")
          .attr("stroke-width", 0.5);

        // Create a container for text clipping
        const textClip = itemGroup.append("g")
          .attr("transform", "translate(25, 0)");

        // Add clipping path for text
        const clipId = `legend-text-clip-${index}`;
        textClip.append("clipPath")
          .attr("id", clipId)
          .append("rect")
          .attr("width", legendWidth - 40)
          .attr("height", legendItemHeight);

        // Add text with clipping and tooltip
        const text = textClip.append("text")
          .attr("y", 12)
          .attr("font-size", "12px")
          .attr("clip-path", `url(#${clipId})`)
          .text(value);

        // Add title element for tooltip on text overflow
        const textNode = text.node();
        if (textNode && textNode.getComputedTextLength() > (legendWidth - 40)) {
          text.append("title").text(value);
        }
      });
    }, [physicalViewData]);

    const renderPhysicalView = useCallback(() => {
      if (!svgRef.current || !physicalViewData) return;

      const svg = d3.select(svgRef.current);
      svg.selectAll("*").remove();

      // Set up zoom behavior with better constraints
      const zoom = d3.zoom<SVGSVGElement, unknown>()
        .scaleExtent([0.1, 2])  // Allow zooming from 0.1x to 2x
        .on("zoom", (event) => {
          inner.attr("transform", event.transform);
        });

      zoomRef.current = zoom;
      svg.call(zoom);

      // Create the main group element
      const inner = svg.append("g");
      graphRef.current = inner;

      // Get nodes from physical view data
      const nodes = Object.entries(physicalViewData.physicalView || {});
      if (nodes.length === 0) return;

      // Calculate layout dimensions
      const svgWidth = parseInt(svg.style("width"));
      const svgHeight = parseInt(svg.style("height"));
      const nodeWidth = 300;
      const nodeHeight = 200;
      const nodeMargin = 50;

      // Calculate grid layout
      const cols = Math.ceil(Math.sqrt(nodes.length));
      const rows = Math.ceil(nodes.length / cols);

      // Calculate total content dimensions
      const contentWidth = cols * (nodeWidth + nodeMargin) + nodeMargin;
      const contentHeight = rows * (nodeHeight + nodeMargin) + nodeMargin;

      // Calculate required scale to fit everything
      const scaleX = svgWidth / contentWidth;
      const scaleY = svgHeight / contentHeight;
      const finalScale = Math.min(scaleX, scaleY, 1) * 0.9; // 0.9 to add some padding

      // Center coordinates
      const centerX = svgWidth / 2;
      const centerY = svgHeight / 2;

      // Get color scale for current context
      const contextValues = getUniqueContextValues(physicalViewData, selectedContext);
      const contextColorScale = getContextColorScale(contextValues);

      // Update the actor coloring in renderPlacementGroups and renderFreeActors
      const getActorColor = (actor: any): string => {
        if (!actor) return "#cccccc";
        
        const contextValue = selectedContext === "actor_id" 
          ? actor.actorId 
          : actor.contextInfo?.[selectedContext]?.toString();
        
        return contextValue ? contextColorScale(contextValue) : "#cccccc";
      };

      // Draw nodes with adjusted positioning
      nodes.forEach(([nodeId, nodeData], index) => {
        const row = Math.floor(index / cols);
        const col = index % cols;
        const x = col * (nodeWidth + nodeMargin) + nodeMargin;
        const y = row * (nodeHeight + nodeMargin) + nodeMargin;

        // Group all placement groups by ID
        const placementGroups: { [pgId: string]: any[] } = {};
        const freeActors: any[] = [];

        // Categorize actors
        if (nodeData?.actors) {
          Object.values(nodeData.actors).forEach((actor) => {
            if (actor?.placementGroup?.id) {
              const pgId = actor.placementGroup.id;
              if (!placementGroups[pgId]) {
                placementGroups[pgId] = [];
              }
              placementGroups[pgId].push(actor);
            } else {
              freeActors.push(actor);
            }
          });
        }

        // Draw node rectangle
        const nodeGroup = inner.append("g")
          .attr("transform", `translate(${x}, ${y})`)
          .attr("class", "node")
          .attr("data-id", nodeId)
          .on("click", (event) => {
            event.stopPropagation();
            onElementClick({ id: nodeId, type: "node", data: nodeData }, true);
          });

        // Node rectangle
        nodeGroup.append("rect")
          .attr("width", nodeWidth)
          .attr("height", nodeHeight)
          .attr("rx", 5)
          .attr("ry", 5)
          .attr("fill", colors.node)
          .attr("stroke", selectedElementId === nodeId ? colors.selectedElement : colors.nodeStroke)
          .attr("stroke-width", selectedElementId === nodeId ? 3 : 1);

        // Node label
        nodeGroup.append("text")
          .attr("x", 10)
          .attr("y", 20)
          .attr("font-weight", "bold")
          .text(`Node: ${nodeId.substring(0, 8)}...`);

        const pgKeys = Object.keys(placementGroups);
        const hasFreeActors = freeActors.length > 0;
        const totalGroups = pgKeys.length + (hasFreeActors ? 1 : 0);
        
        if (totalGroups === 0) return;

        const pgWidth = nodeWidth - 20;
        const pgHeight = (nodeHeight - 60) / totalGroups;

        // Use the new rendering functions with all required parameters
        renderPlacementGroups(
          nodeGroup,
          placementGroups,
          pgKeys,
          nodeData,
          50,
          pgWidth,
          pgHeight,
          colors,
          selectedResource,
          selectedElementId,
          onElementClick,
          getActorColor
        );
        
        renderFreeActors(
          nodeGroup,
          freeActors,
          50 + pgKeys.length * pgHeight,
          pgWidth,
          pgHeight,
          colors,
          selectedElementId,
          onElementClick,
          getActorColor
        );
      });

      // After all nodes are drawn, center the visualization
      svg.call(
        zoom.transform,
        d3.zoomIdentity
          .translate(centerX, centerY)
          .scale(finalScale)
          .translate(-contentWidth/2, -contentHeight/2)
      );

      // Set viewBox to contain the graph
      svg.attr("viewBox", `0 0 ${svgWidth} ${svgHeight}`);

      // Render legend after graph
      renderLegend(selectedContext);
    }, [physicalViewData, selectedElementId, onElementClick, selectedResource, selectedContext, renderLegend]);

    // Initial render and on data change
    useEffect(() => {
      renderPhysicalView();
    }, [renderPhysicalView, physicalViewData, updateKey]);

    return (
      <div ref={ref} className="ray-visualization-container">
        <div className="resource-selector">
          <FormControl fullWidth style={{ marginBottom: '20px' }}>
            <InputLabel>Select Resource Type</InputLabel>
            <Select
              value={selectedResource}
              onChange={(e) => setSelectedResource(e.target.value)}
              label="Select Resource Type"
            >
              {getAvailableResources(physicalViewData.physicalView?.[Object.keys(physicalViewData.physicalView)[0]]?.resources || {})
                .map(({ key, label }) => (
                  <MenuItem key={key} value={key}>
                    {label}
                  </MenuItem>
                ))}
            </Select>
          </FormControl>
          <FormControl fullWidth>
            <InputLabel>Select Context</InputLabel>
            <Select
              value={selectedContext}
              onChange={(e) => setSelectedContext(e.target.value)}
              label="Select Context"
            >
              {getAvailableContextKeys(physicalViewData)
                .map(({ key, label }) => (
                  <MenuItem key={key} value={key}>
                    {label}
                  </MenuItem>
                ))}
            </Select>
          </FormControl>
        </div>
        <div className="graph-container">
          <svg ref={svgRef} width="100%" height="600"></svg>
        </div>
      </div>
    );
  },
);

// Update the rendering of placement groups and actors
const renderPlacementGroups = (
  nodeGroup: any,
  placementGroups: any,
  pgKeys: string[],
  nodeData: any,
  pgY: number,
  pgWidth: number,
  pgHeight: number,
  colors: any,
  selectedResource: string,
  selectedElementId: string | null,
  onElementClick: (data: any, skip_zoom: boolean) => void,
  getActorColor: (actor: any) => string
) => {
  pgKeys.forEach((pgId, pgIndex) => {
    const actors = placementGroups[pgId];
    const currentY = pgY + pgIndex * pgHeight;

    // Placement group rectangle
    const pgGroup = nodeGroup.append("g")
      .attr("transform", `translate(10, ${currentY})`)
      .attr("class", "placement-group")
      .attr("data-id", pgId);

    // Draw the main placement group rectangle
    pgGroup.append("rect")
      .attr("width", pgWidth)
      .attr("height", pgHeight - 5)
      .attr("rx", 3)
      .attr("ry", 3)
      .attr("fill", colors.placementGroup)
      .attr("stroke", colors.placementGroupStroke)
      .attr("stroke-width", 1);

    // Draw resource usage bar if resource is selected
    if (selectedResource) {
      const resourceInfo = extractResourceUsage(nodeData.resources, pgId, selectedResource);
      
      if (resourceInfo) {
        // Background bar
        pgGroup.append("rect")
          .attr("class", "resource-usage-bar resource-usage-bar-background")
          .attr("x", pgWidth)
          .attr("y", 0)
          .attr("width", 8)
          .attr("height", pgHeight - 5);

        // Usage bar with gradient color
        pgGroup.append("rect")
          .attr("class", "resource-usage-bar")
          .attr("x", pgWidth)
          .attr("y", (1 - resourceInfo.usage) * (pgHeight - 5))
          .attr("width", 8)
          .attr("height", resourceInfo.usage * (pgHeight - 5))
          .attr("fill", getResourceUsageColor(resourceInfo.usage));

        // Add usage text
        pgGroup.append("text")
          .attr("class", "resource-usage-text")
          .attr("x", pgWidth + 12)
          .attr("y", pgHeight / 2)
          .attr("text-anchor", "start")
          .attr("dominant-baseline", "middle")
          .attr("font-size", "10px")
          .attr("fill", "#666")
          .text(`${Math.round(resourceInfo.usage * 100)}%`);
      }
    }

    // Calculate actor section dimensions
    const numActors = actors.length;
    const sectionWidth = pgWidth / numActors;

    // Draw actor sections
    actors.forEach((actor: any, actorIndex: number) => {
      if (!actor) return;
      
      const sectionX = actorIndex * sectionWidth;
      
      // Draw section divider lines (except for first section)
      if (actorIndex > 0) {
        pgGroup.append("line")
          .attr("x1", sectionX)
          .attr("y1", 0)
          .attr("x2", sectionX)
          .attr("y2", pgHeight - 5)
          .attr("stroke", colors.placementGroupStroke)
          .attr("stroke-width", 1)
          .attr("stroke-dasharray", "2,2");
      }

      // Create clickable section group
      const sectionGroup = pgGroup.append("g")
        .attr("transform", `translate(${sectionX}, 0)`)
        .attr("class", "actor-section")
        .attr("data-id", actor.actorId || "unknown")
        .on("click", (event: any) => {
          event.stopPropagation();
          onElementClick({ id: actor.actorId || "unknown", type: "actor", data: actor }, true);
        });

      // Calculate actor dimensions with padding
      const padding = 16;
      const actorWidth = sectionWidth - (padding * 2);
      const actorHeight = pgHeight - 5 - (padding * 2);

      // Draw actor background
      sectionGroup.append("rect")
        .attr("x", padding)
        .attr("y", padding)
        .attr("width", actorWidth)
        .attr("height", actorHeight)
        .attr("fill", getActorColor(actor))
        .attr("opacity", 0.7);

      // Add actor label
      sectionGroup.append("text")
        .attr("x", sectionWidth / 2)
        .attr("y", pgHeight / 2)
        .attr("text-anchor", "middle")
        .attr("dominant-baseline", "middle")
        .attr("font-size", "10px")
        .attr("fill", "#000000")
        .text(actor.name !== "Unknown" ? actor.name : `Actor ${actorIndex + 1}`);

      // Highlight if selected
      if (selectedElementId === actor.actorId) {
        sectionGroup.append("rect")
          .attr("x", padding)
          .attr("y", padding)
          .attr("width", actorWidth)
          .attr("height", actorHeight)
          .attr("fill", "none")
          .attr("stroke", colors.selectedElement)
          .attr("stroke-width", 2);
      }
    });
  });
};

// Update the rendering of free actors
const renderFreeActors = (
  nodeGroup: any,
  freeActors: any[],
  freeActorsY: number,
  pgWidth: number,
  pgHeight: number,
  colors: any,
  selectedElementId: string | null,
  onElementClick: (data: any, skip_zoom: boolean) => void,
  getActorColor: (actor: any) => string
) => {
  if (freeActors.length === 0) return;

  const freeActorsGroup = nodeGroup.append("g")
    .attr("transform", `translate(10, ${freeActorsY})`)
    .attr("class", "free-actors");

  // Calculate dimensions for free actor sections
  const sectionWidth = pgWidth / freeActors.length;

  // Draw free actor sections
  freeActors.forEach((actor, actorIndex) => {
    if (!actor) return;
    
    const sectionX = actorIndex * sectionWidth;
    
    // Draw section divider lines (except for first section)
    if (actorIndex > 0) {
      freeActorsGroup.append("line")
        .attr("x1", sectionX)
        .attr("y1", 0)
        .attr("x2", sectionX)
        .attr("y2", pgHeight - 5)
        .attr("stroke", colors.freeActorsStroke)
        .attr("stroke-width", 1)
        .attr("stroke-dasharray", "2,2");
    }

    // Create clickable section group
    const sectionGroup = freeActorsGroup.append("g")
      .attr("transform", `translate(${sectionX}, 0)`)
      .attr("class", "actor-section")
      .attr("data-id", actor.actorId || "unknown")
      .on("click", (event: any) => {
        event.stopPropagation();
        onElementClick({ id: actor.actorId || "unknown", type: "actor", data: actor }, true);
      });

    // Calculate actor dimensions with padding
    const padding = 8;
    const actorWidth = sectionWidth - (padding * 2);
    const actorHeight = pgHeight - 5 - (padding * 2);

    // Draw actor background
    sectionGroup.append("rect")
      .attr("x", padding)
      .attr("y", padding)
      .attr("width", actorWidth)
      .attr("height", actorHeight)
      .attr("fill", getActorColor(actor))
      .attr("opacity", 0.7);

    // Add actor label
    sectionGroup.append("text")
      .attr("x", sectionWidth / 2)
      .attr("y", pgHeight / 2)
      .attr("text-anchor", "middle")
      .attr("dominant-baseline", "middle")
      .attr("font-size", "10px")
      .attr("fill", "#000000")
      .text(actor.name !== "Unknown" ? actor.name : `Actor ${actorIndex + 1}`);

    // Highlight if selected
    if (selectedElementId === actor.actorId) {
      sectionGroup.append("rect")
        .attr("x", padding)
        .attr("y", padding)
        .attr("width", actorWidth)
        .attr("height", actorHeight)
        .attr("fill", "none")
        .attr("stroke", colors.selectedElement)
        .attr("stroke-width", 2);
    }
  });
};

export default PhysicalVisualization; 