import React, { useEffect, useRef } from "react";
import * as d3 from "d3";
import { FlameGraphData } from "../../service/flame-graph";

type FlameVisualizationProps = {
  flameData: FlameGraphData;
  onElementClick: (data: any) => void;
  selectedElementId: string | null;
  jobId?: string;
  updateKey?: number;
  onUpdate?: () => void;
  updating?: boolean;
  searchTerm?: string;
};

type FlameNode = {
  name: string;
  customValue: number;
  originalValue?: number; // Store original value for display
  count?: number;
  children?: FlameNode[];
  hide?: boolean;
  fade?: boolean;
  highlight?: boolean;
}

// Extended type for D3 hierarchy node with partition layout properties
type PartitionHierarchyNode = d3.HierarchyNode<FlameNode> & {
  x0: number;
  x1: number;
  y0: number;
  y1: number;
  delta?: number;
  originalValue?: number;
  id?: string | number;
  customValue?: number; // Make sure this is defined as a mutable property
}

// Generate a color based on the function name
const generateColor = (name: string): string => {
  // Hash the name to generate consistent colors for the same function
  let hash = 0;
  for (let i = 0; i < name.length; i++) {
    hash = name.charCodeAt(i) + ((hash << 5) - hash);
  }
  
  // Generate hue from hash (red to yellow spectrum for flame-like colors)
  const hue = Math.abs(hash) % 50; // 0-50 range gives red-to-yellow
  return `hsl(${hue}, 80%, 50%)`; // High saturation and medium lightness
};

export const FlameVisualization: React.FC<FlameVisualizationProps> = ({
  flameData,
  onElementClick,
  selectedElementId,
  searchTerm,
}) => {
  const containerRef = useRef<HTMLDivElement>(null);
  const tooltipRef = useRef<HTMLDivElement | null>(null);
  const detailsRef = useRef<HTMLDivElement | null>(null);
  const flameChartRef = useRef<any>(null);
  
  useEffect(() => {
    if (!containerRef.current || !flameData) return;

    // Clear previous visualization
    d3.select(containerRef.current).selectAll("*").remove();

    // Create container elements
    const container = d3.select(containerRef.current);
    
    // Create details element
    const detailsElement = container.append("div")
      .attr("class", "flame-details")
      .style("padding", "5px")
      .style("font-family", "Verdana")
      .style("font-size", "12px");
    
    detailsRef.current = detailsElement.node() as HTMLDivElement;

    // Create tooltip
    const tooltip = container.append("div")
      .attr("class", "d3-flame-graph-tip")
      .style("position", "absolute")
      .style("visibility", "hidden")
      .style("pointer-events", "none")
      .style("z-index", "10");
    
    tooltipRef.current = tooltip.node() as HTMLDivElement;

    // Transform data
    const transformedData = transformFlameData(flameData);
    
    // Create flame graph
    const chart: any = createFlameGraph();
    flameChartRef.current = chart;
    
    // Configure chart
    chart
      .width(containerRef.current.offsetWidth)
      .cellHeight(18)
      .minFrameSize(1)
      .transitionDuration(750)
      .transitionEase(d3.easeCubic)
      .inverted(false)
      .onClick((d: PartitionHierarchyNode) => {
        onElementClick({
          id: d.data.name,
          type: "flame",
          data: d.data,
        });
      });
    
    // Set up tooltip
    const tooltipHandler = {
      show: (d: PartitionHierarchyNode, element: SVGGElement) => {
        if (!tooltipRef.current) return;
        
        const tooltip = d3.select(tooltipRef.current);
        const valueInSeconds = d.data.originalValue || 0;
        const formattedValue = valueInSeconds < 0.001 
          ? valueInSeconds.toExponential(2) 
          : valueInSeconds.toFixed(valueInSeconds < 0.1 ? 4 : 2);
        
        tooltip
          .style("visibility", "visible")
          .html(`
            <div>
              <strong>${d.data.name}</strong><br/>
              Duration: ${formattedValue}s<br/>
              ${d.data.count ? `Count: ${d.data.count}` : ""}
            </div>
          `);

        // Position tooltip
        const rect = element.getBoundingClientRect();
        const containerRect = containerRef.current!.getBoundingClientRect();
        
        tooltip
          .style("left", `${rect.left - containerRect.left + rect.width / 2}px`)
          .style("top", `${rect.top - containerRect.top - 30}px`);
      },
      hide: () => {
        if (!tooltipRef.current) return;
        d3.select(tooltipRef.current).style("visibility", "hidden");
      }
    };
    
    chart.tooltip(tooltipHandler);
    
    // Set up details handler
    chart.setDetailsElement(detailsRef.current);
    
    // Apply search if term exists
    if (searchTerm && searchTerm.trim() !== "") {
      chart.search(searchTerm);
    }
    
    // Render chart
    d3.select(containerRef.current)
      .datum(transformedData)
      .call(chart);
    
    // Highlight selected node if any
    if (selectedElementId) {
      const nodes = d3.select(containerRef.current)
        .selectAll<SVGGElement, PartitionHierarchyNode>("g.frame")
        .filter(d => d.data.name === selectedElementId);
      
      if (!nodes.empty()) {
        nodes.select("rect")
          .attr("stroke", "#fff")
          .attr("stroke-width", 2);
        
        // Zoom to selected node
        const node = nodes.datum();
        if (node) {
          chart.zoomTo(node);
        }
      }
    }
    
    // Add CSS for flame graph
    const style = document.createElement('style');
    style.textContent = `
      .d3-flame-graph rect {
        stroke: none;
        fill-opacity: .8;
      }
      
      .d3-flame-graph rect:hover {
        stroke: #474747;
        stroke-width: 0.5;
        cursor: pointer;
      }
      
      .d3-flame-graph-label {
        pointer-events: none;
        white-space: nowrap;
        text-overflow: ellipsis;
        overflow: hidden;
        font-size: 12px;
        font-family: Verdana;
        margin-left: 4px;
        margin-right: 4px;
        line-height: 1.5;
        padding: 0 0 0;
        font-weight: 400;
        color: black;
        text-align: left;
      }
      
      .d3-flame-graph .fade {
        opacity: 0.6 !important;
      }
      
      .d3-flame-graph .title {
        font-size: 20px;
        font-family: Verdana;
      }
      
      .d3-flame-graph-tip {
        background-color: black;
        border: none;
        border-radius: 3px;
        padding: 5px 10px;
        min-width: 250px;
        text-align: left;
        color: white;
        z-index: 10;
      }

      .partition {
        border: none;
        outline: none;
      }
    `;
    document.head.appendChild(style);

    return () => {
      if (containerRef.current) {
        if (flameChartRef.current) {
          flameChartRef.current.destroy();
        }
        d3.select(containerRef.current).selectAll("*").remove();
      }
      document.head.removeChild(style);
    };
  }, [flameData, selectedElementId, searchTerm, onElementClick]);

  const transformFlameData = (data: FlameGraphData): FlameNode => {
    if (!data || !data.aggregated || !Array.isArray(data.aggregated)) {
      console.warn("Invalid flame graph data format:", data);
      return { name: "root", customValue: 0, children: [] };
    }

    console.log("Original flame data:", data);

    // Find max value for normalization
    let maxValue = 0;
    let minValue = Infinity;
    data.aggregated.forEach(node => {
      if (node.value && node.value > maxValue) {
        maxValue = node.value;
      }
      if (node.value && node.value < minValue && node.value > 0) {
        minValue = node.value;
      }
    });
    
    // If no positive values found, set minValue to 0
    if (minValue === Infinity) minValue = 0;
    
    console.log(`Value range: min=${minValue}, max=${maxValue}`);

    // Create a map of function names to their nodes for quick lookup
    const nodeMap = new Map<string, FlameNode>();
    
    // First pass: create all nodes with normalized values
    data.aggregated.forEach(node => {
      // Store the original value for display
      const originalValue = node.value || 0;
      
      // Calculate normalized value for sizing
      // For flame graphs, we want to preserve the relative proportions
      // but ensure small values are still visible
      let normalizedValue = originalValue;
      
      // Apply a more conservative normalization to prevent excessive scaling
      if (normalizedValue > 0 && maxValue > minValue) {
        // For very large ranges, use a more moderate scaling approach
        if (maxValue / minValue > 100) {
          // Use square root scaling for better distribution
          normalizedValue = Math.sqrt(normalizedValue / maxValue) * maxValue;
          
          // Ensure minimum visible size
          if (normalizedValue < maxValue * 0.01) {
            normalizedValue = maxValue * 0.01;
          }
        }
      }
      
      // Create node with empty children array
      nodeMap.set(node.name, {
        name: node.name,
        customValue: normalizedValue,
        originalValue: originalValue,
        count: node.count || 0,
        children: []  // Initialize with empty array
      });
    });
    
    // Add this before the second pass:
    const addedAsChild = new Set<string>();
    
    // Second pass: build the hierarchy
    data.aggregated.forEach(node => {
      const parentNode = nodeMap.get(node.name);
      if (parentNode && node.children) {
        parentNode.children = node.children.map(child => {
          const childName = typeof child === 'string' ? child : child.name;
          let childNode = nodeMap.get(childName);
          
          if (!childNode) {
            childNode = {
              name: childName,
              customValue: 0,
              children: []
            };
            nodeMap.set(childName, childNode);
          }
          addedAsChild.add(childName);  // Track child nodes
          return childNode;
        });
      }
    });
    
    // Build root hierarchy correctly
    const mainNode: FlameNode = {
      name: "root",
      customValue: maxValue,  // Use the pre-calculated maxValue
      children: Array.from(nodeMap.values()).filter(node => 
        !addedAsChild.has(node.name) && node.name !== "root"
      )
    };
    
    // Calculate total value of all children
    let totalChildrenValue = 0;
    if (mainNode.children) {
      mainNode.children.forEach(child => {
        totalChildrenValue += child.customValue || 0;
      });
    }
    
    // If main node value is less than total children value, adjust it
    if (mainNode.customValue < totalChildrenValue) {
      console.log(`Adjusting main node value from ${mainNode.customValue} to ${totalChildrenValue}`);
      mainNode.customValue = totalChildrenValue;
    }
    
    // IMPORTANT: Make sure no nodes are hidden by default
    // This is the key fix - ensure no nodes have hide=true initially
    const ensureNodesVisible = (node: FlameNode) => {
      // Remove hide and fade properties or set them to false
      node.hide = false;
      node.fade = false;
      
      // Recursively process children
      if (node.children && node.children.length > 0) {
        node.children.forEach(child => ensureNodesVisible(child));
      }
    };
    
    // Apply the fix to make all nodes visible
    ensureNodesVisible(mainNode);
    
    // Log the transformed data for debugging
    console.log("Transformed flame data:", mainNode);
    
    return mainNode;
  };

  // Implementation of the flame graph chart function
  const createFlameGraph = () => {
    let width = 960; // graph width
    let height: number | null = null; // graph height
    let cellHeight = 18; // cell height
    let selection: any = null; // selection
    let tooltip: any = null; // tooltip
    let title = ''; // graph title
    let transitionDuration = 750;
    let transitionEase = d3.easeCubic; // tooltip offset
    let sort: boolean | ((a: PartitionHierarchyNode, b: PartitionHierarchyNode) => number) = false;
    let inverted = false; // invert the graph direction
    let clickHandler: ((d: PartitionHierarchyNode) => void) | null = null;
    let hoverHandler: ((d: PartitionHierarchyNode) => void) | null = null;
    let minFrameSize = 0;
    let detailsElement: HTMLElement | null = null;
    let selfValue = false;
    let resetHeightOnZoom = false;
    let minHeight: number | null = null;

    const getName = (d: PartitionHierarchyNode) => {
      return d.data.name;
    };

    const getValue = (d: PartitionHierarchyNode) => {
      // Return the normalized value for sizing
      return d.customValue !== undefined ? d.customValue : (d.value || 0);
    };

    const getChildren = (d: FlameNode) => {
      return d.children;
    };

    const colorMapper = (d: PartitionHierarchyNode) => {
      return d.data.highlight ? '#E600E6' : generateColor(getName(d));
    };


    function hideSiblings(node: PartitionHierarchyNode) {
      let child = node;
      let parent = child.parent as PartitionHierarchyNode;
      
      // First, show the path to the selected node and hide its siblings
      while (parent) {
        parent.data.hide = false; // Ensure the ancestor path is visible
        parent.children?.forEach(sibling => {
          if (sibling !== child) {
            (sibling as PartitionHierarchyNode).data.hide = true;
            // Hide all descendants of hidden siblings
            hideDescendants(sibling as PartitionHierarchyNode);
          }
        });
        
        child = parent;
        parent = parent.parent as PartitionHierarchyNode;
      }

      // Show the selected node and its descendants
      node.data.hide = false;
      if (node.children) {
        node.children.forEach(child => {
          (child as PartitionHierarchyNode).data.hide = false;
        });
      }
    }

    function hideDescendants(node: PartitionHierarchyNode) {
      node.data.hide = true;
      if (node.children) {
        node.children.forEach(child => {
          hideDescendants(child as PartitionHierarchyNode);
        });
      }
    }

    function fadeAncestors(d: PartitionHierarchyNode) {
      if (d.parent) {
        (d.parent as PartitionHierarchyNode).data.fade = true;
        fadeAncestors(d.parent as PartitionHierarchyNode);
      }
    }

    function zoom(d: PartitionHierarchyNode) {
      if (tooltip) tooltip.hide();
      
      // Calculate the scale factor based on root width vs node width
      const root = d.ancestors().pop() || d; // Get the root node
      const scaleFactor = (root.x1 - root.x0) / (d.x1 - d.x0);
      
      // Hide siblings and fade ancestors
      hideSiblings(d);
      fadeAncestors(d);

      // Extend all parent nodes to root.x1
      let current = d;
      while (current.parent) {
        current.parent.x1 = root.x1;
        current.parent.x0 = root.x0;
        current = current.parent;
      }

      // Scale the node and its descendants
      function scaleSubtree(node: PartitionHierarchyNode, baseX0: number, baseX1: number) {
        // For the root node of subtree, use the exact base coordinates
        node.x0 = baseX0;
        node.x1 = baseX1;

        // Handle children sequentially if they exist
        if (node.children && node.children.length > 0) {
          let currentX = node.x0; // Start from parent's x0
          node.children.forEach((child, index) => {
            const childNode = child as PartitionHierarchyNode;
            const childWidth = (childNode.x1 - childNode.x0) * scaleFactor;
            
            // Set child coordinates
            childNode.x0 = currentX;
            childNode.x1 = currentX + childWidth;
            
            // Update currentX for next child
            currentX = childNode.x1;
            
            // Recursively handle this child's children
            if (childNode.children) {
              scaleSubtree(childNode, childNode.x0, childNode.x1);
            }
          });
        }
      }

      // Scale the selected node and its subtree to fill the root width
      scaleSubtree(d, root.x0, root.x1);
      
      update();
      if (typeof clickHandler === 'function') {
        clickHandler(d);
      }
    }

    function searchMatch(d: PartitionHierarchyNode, term: string, ignoreCase = false) {
      if (!term) {
        return false;
      }
      let label = getName(d);
      if (ignoreCase) {
        term = term.toLowerCase();
        label = label.toLowerCase();
      }
      const re = new RegExp(term);
      return typeof label !== 'undefined' && label && label.match(re);
    }

    function searchTree(d: PartitionHierarchyNode, term: string) {
      const results: PartitionHierarchyNode[] = [];
      let sum = 0;

      function searchInner(d: PartitionHierarchyNode, foundParent: boolean) {
        let found = false;

        if (searchMatch(d, term)) {
          d.data.highlight = true;
          found = true;
          if (!foundParent) {
            sum += getValue(d);
          }
          results.push(d);
        } else {
          d.data.highlight = false;
        }

        if (d.children) {
          d.children.forEach(function(child) {
            searchInner(child as PartitionHierarchyNode, (foundParent || found));
          });
        }
      }
      
      searchInner(d, false);
      return [results, sum];
    }

    function clear(d: PartitionHierarchyNode) {
      d.data.highlight = false;
      if (d.children) {
        d.children.forEach(function(child) {
          clear(child as PartitionHierarchyNode);
        });
      }
    }

    function doSort(a: PartitionHierarchyNode, b: PartitionHierarchyNode) {
      if (typeof sort === 'function') {
        return sort(a, b);
      } else if (sort) {
        return d3.ascending(getName(a), getName(b));
      }
      return 0;
    }

    const partition = d3.partition<FlameNode>();

    function filterNodes(root: PartitionHierarchyNode) {
      // Filter out nodes that are marked as hidden
      return root.descendants().filter(node => !node.data.hide);
    }

    function reappraiseNode(root: PartitionHierarchyNode) {
      let node, children, grandChildren, childrenValue, i, j, child, childValue;
      const stack: PartitionHierarchyNode[] = [];
      const included: PartitionHierarchyNode[][] = [];
      const excluded: PartitionHierarchyNode[][] = [];
      const compoundValue = !selfValue;
      let item = root.data;
      
      if (item.hide) {
        root.customValue = 0;
        children = root.children;
        if (children) {
          excluded.push(children as PartitionHierarchyNode[]);
        }
      } else {
        root.customValue = item.fade ? 0 : getValue(root);
        stack.push(root);
      }
      
      // First DFS pass
      while ((node = stack.pop())) {
        children = node.children as PartitionHierarchyNode[];
        if (children && (i = children.length)) {
          childrenValue = 0;
          while (i--) {
            child = children[i];
            item = child.data;
            if (item.hide) {
              child.customValue = 0;
              grandChildren = child.children as PartitionHierarchyNode[];
              if (grandChildren) {
                excluded.push(grandChildren);
              }
              continue;
            }
            if (item.fade) {
              child.customValue = 0;
            } else {
              childValue = getValue(child);
              child.customValue = childValue;
              childrenValue += childValue;
            }
            stack.push(child);
          }
          if (compoundValue && node.customValue) {
            node.customValue -= childrenValue;
          }
          included.push(children);
        }
      }
      
      // Postorder traversal
      i = included.length;
      while (i--) {
        children = included[i];
        childrenValue = 0;
        j = children.length;
        while (j--) {
          childrenValue += children[j].customValue || 0;
        }
        if (children[0] && children[0].parent) {
          (children[0].parent as PartitionHierarchyNode).customValue = ((children[0].parent as PartitionHierarchyNode).customValue || 0) + childrenValue;
        }
      }
      
      // Continue DFS for hidden nodes
      while (excluded.length) {
        children = excluded.pop()!;
        j = children.length;
        while (j--) {
          child = children[j];
          child.customValue = 0;
          grandChildren = child.children as PartitionHierarchyNode[];
          if (grandChildren) {
            excluded.push(grandChildren);
          }
        }
      }
    }

    function update() {
      selection.each(function(this: Element, root: PartitionHierarchyNode) {
        // Create proper scales with domains
        reappraiseNode(root);

        if (sort) root.sort(doSort);

        // Store current coordinates before partition layout
        const nodeMap = new Map<string, {x0: number, x1: number}>();
        root.descendants().forEach(d => {
          if (d.x0 !== undefined && d.x1 !== undefined) {
            nodeMap.set((d as any).pathId, {x0: d.x0, x1: d.x1});
          }
        });

        // Configure partition layout with correct dimensions
        const maxDepth = d3.max(root.descendants(), d => d.depth) || 0;
        const totalHeight = (maxDepth + 1) * cellHeight;
        
        // Apply partition layout only if coordinates haven't been set
        if (root.x0 === undefined || root.x1 === undefined) {
          partition.size([width, totalHeight])(root);
        }

        // Restore scaled coordinates
        root.descendants().forEach(d => {
          const stored = nodeMap.get((d as any).pathId);
          if (stored) {
            d.x0 = stored.x0;
            d.x1 = stored.x1;
          }
        });

        root.x1 = root.children?.[0]?.x1 || root.x1;

        // Calculate width based on the node's proportion of the total width
        function frameWidth(d: PartitionHierarchyNode) { 
          return d.x1 - d.x0; 
        }

        const descendants = filterNodes(root);
        
        // Log some sample node dimensions for debugging
        if (descendants.length > 0) {
          const sample = descendants[0];
          console.log("Sample node dimensions:", {
            x0: sample.x0,
            x1: sample.x1,
            width: sample.x1 - sample.x0,
            depth: sample.depth
          });
        }
        
        const svg = d3.select(this).select('svg');
        svg.attr('width', width);

        let g = svg.selectAll<SVGGElement, PartitionHierarchyNode>('g')
          .data(descendants, d => (d as any).pathId);

        // Set height on first update
        if (!height || resetHeightOnZoom) {
          height = totalHeight + cellHeight; // Add some padding
          if (minHeight && height < minHeight) height = minHeight;
          svg.attr('height', height);
        }

        // Create a proper y scale for vertical positioning
        const yScale = d3.scaleLinear()
          .domain([0, maxDepth])
          .range([0, (height || totalHeight) - cellHeight]);

        // Update existing nodes with correct positioning
        g.transition()
          .duration(transitionDuration)
          .ease(transitionEase)
          .attr('transform', d => 
            `translate(${d.x0},${inverted ? yScale(d.depth) : ((height || totalHeight) - yScale(d.depth) - cellHeight)})`
          );

        g.select('rect')
          .transition()
          .duration(transitionDuration)
          .ease(transitionEase)
          .attr('width', frameWidth);

        // Enter new nodes with correct positioning
        const node = g.enter()
          .append('svg:g')
          .attr('transform', d => 
            `translate(${d.x0},${inverted ? yScale(d.depth) : ((height || totalHeight) - yScale(d.depth) - cellHeight)})`
          );

        node.append('svg:rect')
          .transition()
          .delay(transitionDuration / 2)
          .attr('width', frameWidth);

        if (!tooltip) { 
          node.append('svg:title'); 
        }

        node.append('foreignObject')
          .append('xhtml:div');

        // Re-select to see the new elements
        g = svg.selectAll<SVGGElement, PartitionHierarchyNode>('g')
          .data(descendants, d => (d as any).pathId);

        g.attr('width', frameWidth)
          .attr('height', cellHeight)
          .attr('name', getName)
          .attr('class', d => d.data.fade ? 'frame fade' : 'frame');

        g.select('rect')
          .attr('height', cellHeight)
          .attr('fill', colorMapper);

        g.select('foreignObject')
          .attr('width', frameWidth)
          .attr('height', cellHeight)
          .select('div')
          .attr('class', 'd3-flame-graph-label')
          .style('display', d => (frameWidth(d) < 35) ? 'none' : 'block')
          .transition()
          .delay(transitionDuration)
          .text(getName);

        g.on('click', (event, d) => { 
          event.stopPropagation();
          zoom(d); 
        });

        g.exit().remove();

        g.on('mouseover', function(event, d) {
          if (tooltip) tooltip.show(d, this);
          if (typeof hoverHandler === 'function') {
            hoverHandler(d);
          }
        }).on('mouseout', function() {
          if (tooltip) tooltip.hide();
          if (detailsElement) {
            detailsElement.textContent = '';
          }
        });
      });
    }

    function processData() {
      selection.datum((data: FlameNode) => {
        // Create hierarchy with proper value assignment
        const root = d3.hierarchy(data, getChildren)
          // Use customValue for sizing, falling back to value if not available
          .sum(d => {
            // Ensure we're using the correct value for sizing
            return d.customValue !== undefined ? d.customValue : 0;
          });
        
        // Log the root node for debugging
        console.log("Hierarchy root:", root);

        // Generate path-based IDs
        root.descendants().forEach(node => {
          const path: string[] = [];
          let current: typeof node | null = node;
          while (current && current !== root) {
            path.unshift(current.data.name);
            current = current.parent;
          }
          (node as any).pathId = path.join("->");
        });

        return root as PartitionHierarchyNode;
      });
    }

    function chart(s: any) {
      if (!arguments.length) { return chart; }

      // Saving the selection
      selection = s;

      // Processing raw data
      processData();

      // Create chart svg
      selection.each(function(this: Element) {
        if (d3.select(this).select('svg').size() === 0) {
          const svg = d3.select(this)
            .append('svg:svg')
            .attr('width', width)
            .attr('class', 'partition d3-flame-graph')
            .style('margin', '0 auto')
            .style('display', 'block');

          if (height) {
            if (minHeight && height < minHeight) height = minHeight;
            svg.attr('height', height);
          }

          svg.append('svg:text')
            .attr('class', 'title')
            .attr('text-anchor', 'middle')
            .attr('y', '25')
            .attr('x', width / 2)
            .attr('fill', '#808080')
            .text(title);

          // Only call tooltip if it's a function
          if (tooltip && typeof tooltip === 'function') {
            svg.call(tooltip);
          }
        }
      });

      // First draw
      update();
      
      return chart as any;
    }

    // Chart configuration methods
    chart.width = function(_: number) {
      if (!arguments.length) { return width; }
      width = _;
      return chart;
    };

    chart.height = function(_: number | null) {
      if (!arguments.length) { return height; }
      height = _;
      return chart;
    };

    chart.minHeight = function(_: number | null) {
      if (!arguments.length) { return minHeight; }
      minHeight = _;
      return chart;
    };

    chart.cellHeight = function(_: number) {
      if (!arguments.length) { return cellHeight; }
      cellHeight = _;
      return chart;
    };

    chart.tooltip = function(_: any) {
      if (!arguments.length) { return tooltip; }
      tooltip = _;
      return chart;
    };

    chart.title = function(_: string) {
      if (!arguments.length) { return title; }
      title = _;
      return chart;
    };

    chart.transitionDuration = function(_: number) {
      if (!arguments.length) { return transitionDuration; }
      transitionDuration = _;
      return chart;
    };

    chart.transitionEase = function(_: any) {
      if (!arguments.length) { return transitionEase; }
      transitionEase = _;
      return chart;
    };

    chart.sort = function(_: boolean | ((a: PartitionHierarchyNode, b: PartitionHierarchyNode) => number)) {
      if (!arguments.length) { return sort; }
      sort = _;
      return chart;
    };

    chart.inverted = function(_: boolean) {
      if (!arguments.length) { return inverted; }
      inverted = _;
      return chart;
    };

    chart.minFrameSize = function(_: number) {
      if (!arguments.length) { return minFrameSize; }
      minFrameSize = _;
      return chart;
    };

    chart.setDetailsElement = function(_: HTMLElement | null) {
      if (!arguments.length) { return detailsElement; }
      detailsElement = _;
      return chart;
    };

    chart.selfValue = function(_: boolean) {
      if (!arguments.length) { return selfValue; }
      selfValue = _;
      return chart;
    };

    chart.resetHeightOnZoom = function(_: boolean) {
      if (!arguments.length) { return resetHeightOnZoom; }
      resetHeightOnZoom = _;
      return chart;
    };

    chart.onClick = function(_: ((d: PartitionHierarchyNode) => void) | null) {
      if (!arguments.length) { return clickHandler; }
      clickHandler = _;
      return chart;
    };

    chart.onHover = function(_: ((d: PartitionHierarchyNode) => void) | null) {
      if (!arguments.length) { return hoverHandler; }
      hoverHandler = _;
      return chart;
    };

    chart.search = function(term: string) {
      if (!selection) return;
      
      selection.each(function(data: PartitionHierarchyNode) {
        searchTree(data, term);
      });
      
      update();
    };

    chart.clear = function() {
      if (!selection) return;
      
      if (detailsElement) {
        detailsElement.textContent = '';
      }
      
      selection.each(function(root: PartitionHierarchyNode) {
        clear(root);
      });
      
      update();
    };

    chart.zoomTo = function(d: PartitionHierarchyNode) {
      zoom(d);
    };

    chart.resetZoom = function() {
      if (!selection) return;
      
      selection.each(function(root: PartitionHierarchyNode) {
        zoom(root); // zoom to root
      });
    };

    chart.destroy = function() {
      if (!selection) return chart;
      
      if (tooltip && typeof tooltip.hide === 'function') {
        tooltip.hide();
      }
      
      selection.selectAll('svg').remove();
      return chart;
    };

    return chart as any;
  };

  return (
    <div
      ref={containerRef}
      style={{
        width: "100%",
        height: "500px",
        position: "relative",
        backgroundColor: "transparent",
        fontFamily: "Verdana, sans-serif",
        display: "flex",
        flexDirection: "column",
        alignItems: "center",
        margin: "20px auto",
        maxWidth: "1200px"
      }}
    />
  );
}; 