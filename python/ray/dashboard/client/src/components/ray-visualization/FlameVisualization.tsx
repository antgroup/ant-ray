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
  value: number;
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
  customValue?: number;
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
      .title("Flame Graph")
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
        tooltip
          .style("visibility", "visible")
          .html(`
            <div>
              <strong>${d.data.name}</strong><br/>
              Value: ${d.value ? d.value.toFixed(2) : '0.00'}ms<br/>
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
        stroke: #EEEEEE;
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
      return { name: "root", value: 0, children: [] };
    }

    return {
      name: "root",
      value: 0,
      children: data.aggregated.map(node => ({
        name: node.name,
        value: node.value || 0,
        count: node.count || 0,
        children: (node.children || []).map(child => ({
          name: child.name,
          value: child.value || 0,
        })),
      })),
    };
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
    const searchDetails: (() => void) | null = null;
    let selfValue = false;
    let resetHeightOnZoom = false;
    let minHeight: number | null = null;

    const getName = (d: PartitionHierarchyNode) => {
      return d.data.name;
    };

    const getValue = (d: PartitionHierarchyNode) => {
      return d.customValue !== undefined ? d.customValue : (d.value || 0);
    };

    const getChildren = (d: FlameNode) => {
      return d.children;
    };

    const labelHandler = (d: PartitionHierarchyNode) => {
      return getName(d) + ' (' + d3.format('.3f')(100 * (d.x1 - d.x0)) + '%, ' + getValue(d) + ' ms)';
    };

    const colorMapper = (d: PartitionHierarchyNode) => {
      return d.data.highlight ? '#E600E6' : generateColor(getName(d));
    };

    function show(d: PartitionHierarchyNode) {
      d.data.fade = false;
      d.data.hide = false;
      if (d.children) {
        d.children.forEach(child => show(child as PartitionHierarchyNode));
      }
    }

    function hideSiblings(node: PartitionHierarchyNode) {
      let child = node;
      let parent = child.parent as PartitionHierarchyNode;
      
      while (parent) {
        parent.children?.forEach(sibling => {
          if (sibling !== child) {
            (sibling as PartitionHierarchyNode).data.hide = true;
          }
        });
        
        child = parent;
        parent = parent.parent as PartitionHierarchyNode;
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
      hideSiblings(d);
      show(d);
      fadeAncestors(d);
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
      let nodeList = root.descendants();
      if (minFrameSize > 0) {
        const kx = width / (root.x1 - root.x0);
        nodeList = nodeList.filter(function(el) {
          return ((el.x1 - el.x0) * kx) > minFrameSize;
        });
      }
      return nodeList;
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
        const x = d3.scaleLinear().range([0, width]);
        const y = d3.scaleLinear().range([0, cellHeight]);

        reappraiseNode(root);

        if (sort) root.sort(doSort);

        const rootWithPartition = partition.size([width, 0])(root);

        const kx = width / (rootWithPartition.x1 - rootWithPartition.x0);
        function frameWidth(d: PartitionHierarchyNode) { 
          return (d.x1 - d.x0) * kx; 
        }

        const descendants = filterNodes(rootWithPartition);
        const svg = d3.select(this).select('svg');
        svg.attr('width', width);

        let g = svg.selectAll<SVGGElement, PartitionHierarchyNode>('g')
          .data(descendants, d => d.id!);

        // Set height on first update
        if (!height || resetHeightOnZoom) {
          const maxDepth = Math.max(...descendants.map(n => n.depth));
          height = (maxDepth + 3) * cellHeight;
          if (minHeight && height < minHeight) height = minHeight;
          svg.attr('height', height);
        }

        // Update existing nodes
        g.transition()
          .duration(transitionDuration)
          .ease(transitionEase)
          .attr('transform', d => 
            `translate(${x(d.x0)},${inverted ? y(d.depth) : (height! - y(d.depth) - cellHeight)})`
          );

        g.select('rect')
          .transition()
          .duration(transitionDuration)
          .ease(transitionEase)
          .attr('width', frameWidth);

        // Enter new nodes
        const node = g.enter()
          .append('svg:g')
          .attr('transform', d => 
            `translate(${x(d.x0)},${inverted ? y(d.depth) : (height! - y(d.depth) - cellHeight)})`
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
          .data(descendants, d => d.id!);

        g.attr('width', frameWidth)
          .attr('height', cellHeight)
          .attr('name', getName)
          .attr('class', d => d.data.fade ? 'frame fade' : 'frame');

        g.select('rect')
          .attr('height', cellHeight)
          .attr('fill', colorMapper);

        if (!tooltip) {
          g.select('title')
            .text(labelHandler);
        }

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
          if (detailsElement) {
            detailsElement.textContent = labelHandler(d);
          }
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
        // Creating a root hierarchical structure
        const root = d3.hierarchy(data, getChildren)
          .sum(d => d.value || 0);

        // Augmenting nodes with ids
        let id = 0;
        root.descendants().forEach(node => {
          (node as PartitionHierarchyNode).id = String(id++);
        });

        // Calculate actual value
        reappraiseNode(root as PartitionHierarchyNode);

        // Store value for later use
        (root as PartitionHierarchyNode).originalValue = root.value;

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
            .attr('class', 'partition d3-flame-graph');

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
        backgroundColor: "#fff",
        fontFamily: "Verdana, sans-serif",
      }}
    />
  );
}; 