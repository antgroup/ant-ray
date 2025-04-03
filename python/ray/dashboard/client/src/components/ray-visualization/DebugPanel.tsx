import React, { useState, useEffect, useRef, useMemo } from "react";
import { Breakpoint, getBreakpoints, setBreakpoint, sendDebugCommand, closeDebugSession } from "../../service/debug-insight";
import "./RayVisualization.css";

type DebugPanelProps = {
  open: boolean;
  onClose?: () => void;
  jobId?: string;
  isTab?: boolean;
}

type SourceCodeLine = {
  lineNumber: number;
  content: string;
  isCurrent?: boolean;
  isBreakpoint?: boolean;
}

type StackFrame = {
  filename: string;
  lineNumber: number;
  functionName: string;
}

const DebugPanel: React.FC<DebugPanelProps> = ({ open, onClose, jobId, isTab = false }) => {
  const [breakpoints, setBreakpoints] = useState<Breakpoint[]>([]);
  const [actorClass, setActorClass] = useState("");
  const [actorName, setActorName] = useState("");
  const [methodName, setMethodName] = useState("");
  const [functionName, setFunctionName] = useState("");
  const [currentTaskId, setCurrentTaskId] = useState<string | null>(null);
  const [refreshInterval, setRefreshInterval] = useState<NodeJS.Timeout | null>(null);
  const [autoRefresh, setAutoRefresh] = useState(false);
  const [isLoading, setIsLoading] = useState(false);
  const [selectedBreakpoint, setSelectedBreakpoint] = useState<Breakpoint | null>(null);
  const [isDisablingBreakpoint, setIsDisablingBreakpoint] = useState(false);

  // New state for IDE-like debug interface
  const [sourceCode, setSourceCode] = useState<SourceCodeLine[]>([]);
  const [currentLine, setCurrentLine] = useState<number | null>(null);
  const [stackFrames, setStackFrames] = useState<StackFrame[]>([]);
  const [currentFile, setCurrentFile] = useState<string | null>(null);
  const [executing, setExecuting] = useState(false);
  const [executionMessage, setExecutionMessage] = useState<string | null>(null);
  const [output, setOutput] = useState<string>('');

  // Add state for foldable sections
  const [breakpointSectionOpen, setBreakpointSectionOpen] = useState(true);
  const [breakpointListOpen, setBreakpointListOpen] = useState(true);
  const [sourceCodeSectionOpen, setSourceCodeSectionOpen] = useState(true);
  const [stackSectionOpen, setStackSectionOpen] = useState(true);
  const [outputSectionOpen, setOutputSectionOpen] = useState(true);
  
  const [isClosingSession, setIsClosingSession] = useState(false);
  
  // State to store mapping from 'filename:lineNumber' to breakpoint index
  const [lineBreakpoints, setLineBreakpoints] = useState<Map<string, number>>(new Map());
  
  const sourceCodeRef = useRef<HTMLDivElement>(null);
  const outputRef = useRef<HTMLDivElement>(null);
  
  useEffect(() => {
    if (open) {
      fetchBreakpoints();
    }
    
    return () => {
      if (refreshInterval) {
        clearInterval(refreshInterval);
      }
    };
  }, [open, jobId]);
  
  useEffect(() => {
    if (autoRefresh) {
      const interval = setInterval(() => {
        fetchBreakpoints();
      }, 5000);
      setRefreshInterval(interval);
    } else if (refreshInterval) {
      clearInterval(refreshInterval);
      setRefreshInterval(null);
    }
    
    return () => {
      if (refreshInterval) {
        clearInterval(refreshInterval);
      }
    };
  }, [autoRefresh]);
  
  
  // Auto scroll output to bottom when updated
  useEffect(() => {
    if (outputRef.current) {
      outputRef.current.scrollTop = outputRef.current.scrollHeight;
    }
  }, [output]);

  // When a breakpoint is selected, fetch source code
  useEffect(() => {
    if (selectedBreakpoint && selectedBreakpoint.taskId) {
      fetchSourceCode();
      // Clear the output when switching breakpoints
      setOutput('');
    }
  }, [selectedBreakpoint]);

  // Add an effect to update breakpoints in UI when sourceCode changes
  useEffect(() => {
    if (selectedBreakpoint && selectedBreakpoint.taskId && sourceCode.length > 0 && currentFile) {
      // If we have source code loaded but no breakpoints marked, fetch breakpoints
      const hasBreakpointsMarked = sourceCode.some(line => line.isBreakpoint);
      if (!hasBreakpointsMarked) {
        console.log("Source code loaded but no breakpoints marked, fetching breakpoints...");
        fetchAndRecordAllBreakpoints();
      }
    }
  }, [sourceCode, currentFile]);

  // Add effect to handle initial load
  useEffect(() => {
    if (open && selectedBreakpoint && selectedBreakpoint.taskId) {
      console.log("Debug panel opened with active breakpoint, ensuring breakpoints are loaded");
      fetchAndRecordAllBreakpoints();
    }
  }, [open]);

  // Scroll to current line in source code view
  useEffect(() => {
    if (sourceCodeRef.current && currentLine !== null) {
      const lineElement = sourceCodeRef.current.querySelector(`[data-line="${currentLine}"]`);
      if (lineElement) {
        lineElement.scrollIntoView({ behavior: 'smooth', block: 'center' });
      }
    }
  }, [currentLine, sourceCode]);

  // Execute debug command and handle the response
  const executeDebugCommand = async (cmd: string): Promise<string | null> => {
    if (!cmd.trim() || !jobId || !currentTaskId || !selectedBreakpoint) return null;
    
    try {
      setExecuting(true);
      setExecutionMessage(`Executing: ${cmd}...`);
      
      const response = await sendDebugCommand({
        job_id: jobId,
        task_id: currentTaskId,
        cmd: cmd + '\n'  // Add newline for command termination
      });
      
      console.log("Debug command response:", response);
      
      parseDebugOutput(response);
      
      // If this is a command that changes execution state, refetch source code
      if (['n', 'next', 's', 'step', 'c', 'continue', 'r', 'return'].includes(cmd)) {
        await fetchSourceCode();
      }
      
      setExecutionMessage(null);
      return response;
    } catch (error) {
      console.error("Failed to send debug command:", error);
      setExecutionMessage("Error executing command");
      return null;
    } finally {
      setExecuting(false);
    }
  };

  // Parse debug output to extract source code, variables, and stack
  const parseDebugOutput = (output: string) => {
    if (!output) return;

    // Track if we found any source code lines
    let foundSourceCode = false;
    const parsedLines: SourceCodeLine[] = [];
    
    // Direct string parsing without regex
    const lines = output.split('\n');
    let currentLine = null;
    
    for (let i = 0; i < lines.length; i++) {
      const line = lines[i].trim();
      
      // Look for lines with format like "24  ->" or "24    " that indicate source code
      if (line.length > 0 && !isNaN(parseInt(line.charAt(0), 10))) {
        const firstSpaceIndex = line.indexOf(' ');
        if (firstSpaceIndex > 0) {
          const lineNumber = parseInt(line.substring(0, firstSpaceIndex), 10);
          if (!isNaN(lineNumber)) {
            const remainingText = line.substring(firstSpaceIndex).trim();
            const isArrowLine = remainingText.startsWith("->");
            
            // If it's a line with an arrow, it's the current line
            if (isArrowLine) {
              currentLine = lineNumber;
              // Content starts after the arrow
              const content = remainingText.substring(2).trim();
              parsedLines.push({
                lineNumber,
                content,
                isCurrent: true,
                isBreakpoint: false
              });
              setCurrentLine(lineNumber);
              foundSourceCode = true;
            } 
            // Otherwise it's a regular source code line
            else {
              parsedLines.push({
                lineNumber,
                content: remainingText,
                isCurrent: lineNumber === currentLine,
                isBreakpoint: false
              });
              foundSourceCode = true;
            }
          }
        }
      }
      
      // Look for file location information
      if (line.startsWith('>') && line.includes('(') && line.includes(')')) {
        // Format is like: "> /path/to/file.py(42)(some_function)"
        const arrowRemoved = line.substring(1).trim();
        const openParenIndex = arrowRemoved.indexOf('(');
        
        if (openParenIndex > 0) {
          const filename = arrowRemoved.substring(0, openParenIndex).trim();
          const closeParenIndex = arrowRemoved.indexOf(')', openParenIndex);
          
          if (closeParenIndex > openParenIndex) {
            const lineNumberStr = arrowRemoved.substring(openParenIndex + 1, closeParenIndex);
            const lineNumber = parseInt(lineNumberStr, 10);
            
            if (!isNaN(lineNumber)) {
              setCurrentFile(filename);
              setCurrentLine(lineNumber);
              
              // Try to extract function name
              const remainingText = arrowRemoved.substring(closeParenIndex + 1).trim();
              const funcOpenParenIndex = remainingText.indexOf('(');
              const funcCloseParenIndex = remainingText.indexOf(')');
              
              if (funcOpenParenIndex >= 0 && funcCloseParenIndex > funcOpenParenIndex) {
                const functionName = remainingText.substring(funcOpenParenIndex + 1, funcCloseParenIndex);
                
                // Update stack frames if this is a new function
                setStackFrames(prev => {
                  const newFrame = { 
                    filename, 
                    lineNumber, 
                    functionName 
                  };
                  
                  // Check if this frame is already in the stack
                  if (!prev.some(frame => 
                    frame.filename === newFrame.filename && 
                    frame.lineNumber === newFrame.lineNumber && 
                    frame.functionName === newFrame.functionName
                  )) {
                    return [...prev, newFrame];
                  }
                  return prev;
                });
              }
            }
          }
        }
      }
    }
    
    // Only update source code if we found some lines
    if (foundSourceCode && parsedLines.length > 0) {
      // Preserve breakpoint status when updating source code
      if (sourceCode.length > 0) {
        const breakpointLines = new Set(
          sourceCode
            .filter(line => line.isBreakpoint)
            .map(line => line.lineNumber)
        );
        
        // Update parsedLines to maintain breakpoint status
        parsedLines = parsedLines.map(line => ({
          ...line,
          isBreakpoint: breakpointLines.has(line.lineNumber) || line.isBreakpoint
        }));
      }
      
      setSourceCode(parsedLines);
    }
  };

  // New function to fetch and record all breakpoints from PDB
  const fetchAndRecordAllBreakpoints = async () => {
    if (!selectedBreakpoint || !selectedBreakpoint.taskId) return;
    
    try {
      setExecuting(true);
      setExecutionMessage("Fetching breakpoints...");
      
      const response = await sendDebugCommand({
        job_id: jobId,
        task_id: selectedBreakpoint.taskId,
        cmd: "b\n"  // List all breakpoints in pdb
      });
      
      if (response) {
        // Create a new map to store breakpoint information
        const newBreakpointMap = new Map<string, number>();
        
        // Parse the response to extract breakpoint information
        const lines = response.split('\n');
        
        // Skip the header line
        for (let i = 1; i < lines.length; i++) {
          const line = lines[i].trim();
          if (!line || line === "(Pdb) ") continue;
          
          try {
            // First, extract the breakpoint number from the beginning of the line
            const firstSpaceIndex = line.indexOf(' ');
            if (firstSpaceIndex < 1) continue;
            
            const bpIndexStr = line.substring(0, firstSpaceIndex).trim();
            const bpIndex = parseInt(bpIndexStr, 10);
            if (isNaN(bpIndex)) continue;
            
            // Find 'at' in the line followed by the file path and line number
            const atIndex = line.indexOf(' at ');
            if (atIndex < 0) continue;
            
            // Extract everything after 'at ' which is the file path and line number
            const locationPart = line.substring(atIndex + 4).trim();
            const lastColonIndex = locationPart.lastIndexOf(':');
            
            if (lastColonIndex > 0) {
              const filename = locationPart.substring(0, lastColonIndex);
              const lineNumber = parseInt(locationPart.substring(lastColonIndex + 1), 10);
              
              if (!isNaN(lineNumber)) {
                const key = `${filename}:${lineNumber}`;
                console.log(`Storing breakpoint index ${bpIndex} for ${key}`);
                newBreakpointMap.set(key, bpIndex);
                
                // If we don't have a current file yet but we have breakpoints, set it
                if (!currentFile && filename) {
                  console.log("Setting current file to first breakpoint file:", filename);
                  setCurrentFile(filename);
                }
                
                // Update source code to show this breakpoint if it's in the current file
                // Note: currentFile might be null initially, so we need to handle that
                if (currentFile && (currentFile === filename || filename.endsWith(currentFile) || currentFile.endsWith(filename))) {
                  console.log("Updating source code for breakpoint at line", lineNumber);
                  setSourceCode(prev => prev.map(srcLine => 
                    srcLine.lineNumber === lineNumber 
                      ? { ...srcLine, isBreakpoint: true } 
                      : srcLine
                  ));
                }
              }
            }
          } catch (error) {
            console.error("Error parsing breakpoint line:", line, error);
          }
        }
        
        // Update the lineBreakpoints state with the new map
        setLineBreakpoints(newBreakpointMap);
      }
      
      setExecutionMessage(null);
    } catch (error) {
      console.error("Failed to fetch breakpoints:", error);
      setExecutionMessage("Error fetching breakpoints");
    } finally {
      setExecuting(false);
    }
    
    return true; // Return success for callers to know it completed
  };

  // Update fetchSourceCode to set currentFile before fetching breakpoints
  const fetchSourceCode = async () => {
    if (!selectedBreakpoint || !selectedBreakpoint.taskId) return;
    
    try {
      setExecuting(true);
      setExecutionMessage("Fetching source code...");
      
      // First, get the current execution point
      const whereResponse = await sendDebugCommand({
        job_id: jobId,
        task_id: selectedBreakpoint.taskId,
        cmd: "where\n"
      });
      
      if (whereResponse) {
        parseDebugOutput(whereResponse);
      }
      
      // Get source code listing
      const listResponse = await sendDebugCommand({
        job_id: jobId,
        task_id: selectedBreakpoint.taskId,
        cmd: "ll\n"
      });
      
      if (listResponse) {
        parseDebugOutput(listResponse);
      }
      
      // Ensure we have a currentFile before fetching breakpoints
      if (!currentFile) {
        // Try to get current file from where command response
        const whereLines = whereResponse ? whereResponse.split('\n') : [];
        for (const line of whereLines) {
          if (line.trimStart().startsWith('>') && line.includes('(')) {
            const pathMatch = line.match(/>\s+([^(]+)\(/);
            if (pathMatch && pathMatch[1]) {
              setCurrentFile(pathMatch[1].trim());
              break;
            }
          }
        }
      }
      
      // Get all current breakpoints to update the source code view
      await fetchAndRecordAllBreakpoints();
      
      setExecutionMessage(null);
    } catch (error) {
      console.error("Failed to fetch source code:", error);
      setExecutionMessage("Error fetching source code");
    } finally {
      setExecuting(false);
    }
  };

  const handleSetBreakpoint = async () => {
    if ((!actorClass && !functionName) || (actorClass && !methodName)) {
      alert("Please fill required fields");
      return;
    }
    
    try {
      setIsLoading(true);
      const result = await setBreakpoint({
        job_id: jobId,
        actor_cls: actorClass || undefined,
        actor_name: actorName || undefined,
        method_name: methodName || undefined,
        func_name: functionName || undefined,
        flag: true,
      });
      
      if (result) {
        // Clear form fields
        setActorClass("");
        setActorName("");
        setMethodName("");
        setFunctionName("");
        
        // Refresh breakpoints list
        fetchBreakpoints();
      } else {
        alert("Failed to set breakpoint. Please check the console for details.");
      }
    } catch (error) {
      console.error("Failed to set breakpoint:", error);
      alert("An error occurred while setting the breakpoint");
    } finally {
      setIsLoading(false);
    }
  };

  const handleDisableBreakpoint = async (bp: Breakpoint) => {
    try {
      setIsDisablingBreakpoint(true);
      const result = await setBreakpoint({
        job_id: jobId,
        actor_cls: bp.actorCls || undefined,
        actor_name: bp.actorName || undefined,
        method_name: bp.methodName || undefined,
        func_name: bp.funcName || undefined,
        flag: false,
      });
      
      if (result) {
        if (bp.taskId && bp.taskId !== 'unknown' && selectedBreakpoint && selectedBreakpoint.taskId === bp.taskId) {
          await closeDebugSession(jobId!, bp.taskId);
          setSelectedBreakpoint(null);
          setCurrentTaskId(null);
          
          // Clear IDE-related state
          setSourceCode([]);
          setCurrentFile(null);
          setCurrentLine(null);
        }
        fetchBreakpoints();
      } else {
        alert("Failed to disable breakpoint. Please check the console for details.");
      }
    } catch (error) {
      console.error("Failed to disable breakpoint:", error);
      alert("An error occurred while disabling the breakpoint");
    } finally {
      setIsDisablingBreakpoint(false);
    }
  };

  const handleDeleteBreakpoint = async (bp: Breakpoint) => {
    try {
      setIsLoading(true);
      
      // Only call closeDebugSession if the task_id is valid and not 'unknown'
      let result = true;
      if (bp.taskId && bp.taskId !== 'unknown') {
        result = await closeDebugSession(jobId!, bp.taskId);
      } else {
        // For non-active breakpoints or ones with unknown task_id, just disable them
        result = await setBreakpoint({
          job_id: jobId,
          actor_cls: bp.actorCls || undefined,
          actor_name: bp.actorName || undefined,
          method_name: bp.methodName || undefined,
          func_name: bp.funcName || undefined,
          flag: false,
        });
      }
      
      if (result) {
        // If this was the selected breakpoint, clear the selection
        if (selectedBreakpoint && selectedBreakpoint.taskId === bp.taskId) {
          setSelectedBreakpoint(null);
          setCurrentTaskId(null);
          
          // Clear IDE-related state
          setSourceCode([]);
          setCurrentFile(null);
          setCurrentLine(null);
        }
        
        // Refresh breakpoints list
        fetchBreakpoints();
      } else {
        alert("Failed to delete breakpoint. Please check the console for details.");
      }
    } catch (error) {
      console.error("Failed to delete breakpoint:", error);
      alert("An error occurred while deleting the breakpoint");
    } finally {
      setIsLoading(false);
    }
  };

  const handleSelectBreakpoint = (bp: Breakpoint) => {
    if (bp.taskId && bp.taskId !== 'unknown') {
      if (!selectedBreakpoint || selectedBreakpoint.taskId !== bp.taskId) {
        setSelectedBreakpoint(bp);
        setCurrentTaskId(bp.taskId);
        
        // Clear previous debugging state
        setSourceCode([]);
        setCurrentFile(null);
        setCurrentLine(null);
        
        // Force breakpoint refresh after a short delay to allow state to update
        setTimeout(() => {
          fetchSourceCode().then(() => {
            fetchAndRecordAllBreakpoints();
          });
        }, 100);
      }
    }
  };

  // Debug command functions
  const handleStepOver = async () => {
    const response = await executeDebugCommand('n');
    if (response) {
      // Output already updated in executeDebugCommand
    }
  };

  const handleStepInto = async () => {
    const response = await executeDebugCommand('s');
    if (response) {
      // Output already updated in executeDebugCommand
    }
  };

  const handleStepOut = async () => {
    const response = await executeDebugCommand('r');
    if (response) {
      // Output already updated in executeDebugCommand
    }
  };

  const handleContinue = async () => {
    const response = await executeDebugCommand('c');
    if (response) {
      // Output already updated in executeDebugCommand
    }
  };

  const handleEvaluate = async (expr: string) => {
    if (!expr.trim() || !selectedBreakpoint || !selectedBreakpoint.taskId) return;
    
    try {
      setExecuting(true);
      setExecutionMessage(`Evaluating expression...`);
      
      const response = await sendDebugCommand({
        job_id: jobId,
        task_id: selectedBreakpoint.taskId,
        cmd: `p ${expr}\n`
      });
      
      if (response) {
        // Only show the latest evaluation result
        setOutput(response.trim());
      }
      
      setExecutionMessage(null);
    } catch (error) {
      console.error("Failed to evaluate expression:", error);
      setExecutionMessage("Error evaluating expression");
    } finally {
      setExecuting(false);
    }
  };

  const fetchBreakpoints = async () => {
    try {
      setIsLoading(true);
      const breakpointsData = await getBreakpoints(jobId);
      console.log("Fetched breakpoints:", breakpointsData);
      
      // Use breakpoints directly since getBreakpoints now handles the response format
      setBreakpoints(breakpointsData);
      
      // Find active breakpoints with task_id set and not 'unknown'
      const activeBreakpoints = breakpointsData.filter(bp => bp && bp.enable && bp.taskId && bp.taskId !== 'unknown');
      
      if (activeBreakpoints.length > 0) {
        // Set current task ID to the first active breakpoint task ID
        const firstActive = activeBreakpoints[0];
        if (firstActive.taskId) {
          setCurrentTaskId(firstActive.taskId);
        }
        
        // If there's no selected breakpoint yet or the current selected breakpoint is no longer active,
        // select the first active one
        if (!selectedBreakpoint || !activeBreakpoints.some(bp => bp.taskId === selectedBreakpoint.taskId)) {
          console.log("Setting selected breakpoint to:", firstActive);
          setSelectedBreakpoint(firstActive);
        }
      } else {
        // No active breakpoints, clear current task ID
        setCurrentTaskId(null);
        // If there was a selected breakpoint but it's no longer active, clear it
        if (selectedBreakpoint && selectedBreakpoint.taskId) {
          setSelectedBreakpoint(null);
        }
      }
    } catch (error) {
      console.error("Failed to fetch breakpoints:", error);
      // If there's an error, set an empty array to avoid "map" errors
      setBreakpoints([]);
    } finally {
      setIsLoading(false);
    }
  };

  // New function to get breakpoint index
  // Update handleSetLineBreakpoint to record the breakpoint in our map
  const handleSetLineBreakpoint = async (filename: string, lineNumber: number) => {
    if (!currentFile || !selectedBreakpoint || !selectedBreakpoint.taskId) return;
    
    try {
      setExecuting(true);
      setExecutionMessage(`Setting breakpoint at line ${lineNumber}...`);
      
      // Use the Pdb 'b' command to set a breakpoint at the specified line
      const response = await sendDebugCommand({
        job_id: jobId,
        task_id: selectedBreakpoint.taskId,
        cmd: `b ${lineNumber}\n`
      });
      
      console.log("Set line breakpoint response:", response);
      
      // After setting a breakpoint, update our map with all breakpoints
      await fetchAndRecordAllBreakpoints();
      
      setExecutionMessage(null);
    } catch (error) {
      console.error("Failed to set line breakpoint:", error);
      setExecutionMessage("Error setting breakpoint");
    } finally {
      setExecuting(false);
    }
  };

  // Update handleClearLineBreakpoint to use the breakpoint index from our map
  const handleClearLineBreakpoint = async (filename: string, lineNumber: number) => {
    if (!currentFile || !selectedBreakpoint || !selectedBreakpoint.taskId) return;
    
    const key = `${filename}:${lineNumber}`;
    const bpIndex = lineBreakpoints.get(key);

    if (bpIndex === undefined) {
      console.error("Could not find stored breakpoint index for line", lineNumber);
      // Fetch all breakpoints as a fallback in case our map is out of sync
      await fetchAndRecordAllBreakpoints();
      // Try again with the updated map
      const updatedBpIndex = lineBreakpoints.get(key);
      if (updatedBpIndex === undefined) {
        setExecutionMessage("Could not find breakpoint to clear");
        setTimeout(() => setExecutionMessage(null), 2000);
        return;
      }
    }
    
    try {
      setExecuting(true);
      setExecutionMessage(`Clearing breakpoint at line ${lineNumber}...`);
      
      // Use the Pdb 'clear' command with the stored breakpoint index
      const response = await sendDebugCommand({
        job_id: jobId,
        task_id: selectedBreakpoint.taskId,
        cmd: `clear ${bpIndex || lineNumber}\n`
      });
      
      console.log("Clear breakpoint response:", response);
      
      // Immediately update the UI to remove the breakpoint indicator
      setSourceCode(prev => prev.map(line => 
        line.lineNumber === lineNumber 
          ? { ...line, isBreakpoint: false } 
          : line
      ));
      
      // Remove the breakpoint from the stored map
      setLineBreakpoints(prev => {
        const newMap = new Map(prev);
        newMap.delete(key);
        return newMap;
      });
      
      // After clearing, update our map with all current breakpoints
      await fetchAndRecordAllBreakpoints();
      
      setExecutionMessage(null);
    } catch (error) {
      console.error("Failed to clear breakpoint:", error);
      setExecutionMessage("Error clearing breakpoint");
    } finally {
      setExecuting(false);
    }
  };

  // Toggle a breakpoint at a specific line
  const toggleLineBreakpoint = async (lineNumber: number) => {
    if (!currentFile || !selectedBreakpoint || !selectedBreakpoint.taskId) return;
    
    // Find the line in source code
    const lineData = sourceCode.find(line => line.lineNumber === lineNumber);
    if (!lineData) return;
    
    try {
      if (lineData.isBreakpoint) {
        await handleClearLineBreakpoint(currentFile, lineNumber);
      } else {
        await handleSetLineBreakpoint(currentFile, lineNumber);
      }
      
      // Always fetch breakpoints after toggling to ensure UI is in sync
      await fetchAndRecordAllBreakpoints();
    } catch (error) {
      console.error("Error toggling breakpoint:", error);
    }
  };

  if (!open) return null;

  const renderContent = () => {
    return (
      <>
        {!isTab && (
          <div className="debug-panel-header">
            <h3>Debug Panel</h3>
            {onClose && (
              <button 
                className="close-button" 
                onClick={onClose}
                aria-label="Close"
              >
                ✕
              </button>
            )}
          </div>
        )}
        
        <div className="debug-panel-section">
          <div className="section-header">
            <h4>
              <button 
                className="toggle-button" 
                onClick={() => setBreakpointSectionOpen(!breakpointSectionOpen)}
                aria-label={breakpointSectionOpen ? "Collapse section" : "Expand section"}
              >
                {breakpointSectionOpen ? "▼" : "►"}
              </button>
              Set Breakpoint
            </h4>
          </div>
           
          {breakpointSectionOpen && (
            <div className="form-compact">
              <div className="form-row">
                <div className="breakpoint-type-selector">
                  <div className="selector-item">
                    <input
                      id="actor-type"
                      type="radio"
                      checked={!functionName}
                      onChange={() => {
                        setFunctionName("");
                      }}
                      disabled={isLoading}
                    />
                    <label htmlFor="actor-type">Actor Method</label>
                  </div>
                  <div className="selector-item">
                    <input
                      id="function-type"
                      type="radio"
                      checked={!!functionName}
                      onChange={() => {
                        setActorClass("");
                        setActorName("");
                        setMethodName("");
                        setFunctionName("remote_function");
                      }}
                      disabled={isLoading}
                    />
                    <label htmlFor="function-type">Function</label>
                  </div>
                </div>
              </div>
              
              {!functionName ? (
                <div className="actor-inputs">
                  <div className="form-row compact">
                    <input
                      type="text"
                      placeholder="Actor Class"
                      value={actorClass}
                      onChange={(e) => setActorClass(e.target.value)}
                      disabled={isLoading}
                    />
                    <input
                      type="text"
                      placeholder="Actor Name (optional)"
                      value={actorName}
                      onChange={(e) => setActorName(e.target.value)}
                      disabled={isLoading}
                    />
                    <input
                      type="text"
                      placeholder="Method Name"
                      value={methodName}
                      onChange={(e) => setMethodName(e.target.value)}
                      disabled={isLoading}
                    />
                  </div>
                </div>
              ) : (
                <div className="function-input">
                  <div className="form-row compact">
                    <input
                      type="text"
                      placeholder="Function Name"
                      value={functionName}
                      onChange={(e) => setFunctionName(e.target.value)}
                      disabled={isLoading}
                    />
                  </div>
                </div>
              )}
              
              <div className="form-row">
                <button 
                  className="primary-button small"
                  onClick={handleSetBreakpoint}
                  disabled={isLoading}
                >
                  {isLoading ? "Setting..." : "Set Breakpoint"}
                </button>
              </div>
            </div>
          )}
        </div>
        
        <div className="debug-panel-section">
          <div className="section-header">
            <h4>
              <button 
                className="toggle-button" 
                onClick={() => setBreakpointListOpen(!breakpointListOpen)}
                aria-label={breakpointListOpen ? "Collapse section" : "Expand section"}
              >
                {breakpointListOpen ? "▼" : "►"}
              </button>
              Breakpoints
            </h4>
            <div className="auto-refresh">
              <button 
                className="refresh-button"
                onClick={fetchBreakpoints}
                disabled={isLoading}
                title="Refresh Breakpoints"
              >
                🔄
              </button>
              <label htmlFor="auto-refresh">Auto</label>
              <input
                id="auto-refresh"
                type="checkbox"
                checked={autoRefresh}
                onChange={(e: React.ChangeEvent<HTMLInputElement>) => setAutoRefresh(e.target.checked)}
                disabled={isLoading}
              />
            </div>
          </div>
          
          {breakpointListOpen && (
            <div className="breakpoint-list">
              {isLoading ? (
                <div className="loading-message">Loading breakpoints...</div>
              ) : !Array.isArray(breakpoints) || breakpoints.length === 0 ? (
                <div className="empty-message">No breakpoints set</div>
              ) : (
                <ul>
                  {breakpoints.map((bp, index) => (
                    <li 
                      key={index} 
                      className={`breakpoint-item ${bp.taskId && bp.taskId !== 'unknown' ? 'active-breakpoint' : ''} ${selectedBreakpoint && selectedBreakpoint.taskId === bp.taskId ? 'selected-breakpoint' : ''}`}
                      onClick={() => bp.taskId && bp.taskId !== 'unknown' && handleSelectBreakpoint(bp)}
                      title={bp.taskId && bp.taskId !== 'unknown' ? "Click to select this active breakpoint for debugging" : ""}
                    >
                      <span>
                        {bp.funcName ? (
                          `Function: ${bp.funcName || 'unknown'}`
                        ) : (
                          `${bp.actorCls || 'unknown'}${bp.actorName ? '.' + bp.actorName : ''}.${bp.methodName || 'unknown'}`
                        )}
                        {bp.enable ? ' (Enabled)' : ' (Disabled)'}
                        {bp.taskId && bp.taskId !== 'unknown' ? ' (Active ⚡)' : ''}
                        {selectedBreakpoint && selectedBreakpoint.taskId === bp.taskId ? ' (Selected ✓)' : ''}
                      </span>
                      <div className="breakpoint-actions">
                        <button 
                          className="disable-button"
                          onClick={(e) => {
                            e.stopPropagation();
                            handleDisableBreakpoint(bp);
                          }}
                          aria-label="Disable breakpoint"
                          disabled={isLoading || isDisablingBreakpoint}
                          title={bp.taskId && bp.taskId !== 'unknown' ? "Close debug session and disable breakpoint" : "Disable breakpoint"}
                        >
                          {bp.taskId && bp.taskId !== 'unknown' ? "⏹️" : "⏸️"}
                        </button>
                        <button 
                          className="delete-button"
                          onClick={(e) => {
                            e.stopPropagation();
                            handleDeleteBreakpoint(bp);
                          }}
                          aria-label="Delete breakpoint"
                          disabled={isLoading}
                        >
                          ×
                        </button>
                      </div>
                    </li>
                  ))}
                </ul>
              )}
            </div>
          )}
        </div>

        {/* Debug controls */}
        {selectedBreakpoint && selectedBreakpoint.taskId && (
          <>
            <div className="active-session">
              <div className="active-session-info">
                <div>
                  <strong>Active Session:</strong> {currentTaskId}
                </div>
                {selectedBreakpoint && (
                  <div>
                    <strong>Selected Breakpoint:</strong> {selectedBreakpoint.funcName ? 
                      `Function: ${selectedBreakpoint.funcName || 'unknown'}` : 
                      `${selectedBreakpoint.actorCls || 'unknown'}${selectedBreakpoint.actorName ? '.' + selectedBreakpoint.actorName : ''}.${selectedBreakpoint.methodName || 'unknown'}`
                    }
                  </div>
                )}
                {currentFile && (
                  <div>
                    <strong>File:</strong> {currentFile}
                  </div>
                )}
              </div>
            </div>

            {/* Debug Control Buttons */}
            <div className="debug-controls">
              <button 
                className="debug-button"
                onClick={handleStepOver}
                disabled={executing}
                title="Step Over (n)"
              >
                Step Over (n)
              </button>
              <button 
                className="debug-button"
                onClick={handleStepInto}
                disabled={executing}
                title="Step Into (s)"
              >
                Step Into (s)
              </button>
              <button 
                className="debug-button"
                onClick={handleStepOut}
                disabled={executing}
                title="Step Out (r)"
              >
                Step Out (r)
              </button>
              <button 
                className="debug-button"
                onClick={handleContinue}
                disabled={executing}
                title="Continue (c)"
              >
                Continue (c)
              </button>
            </div>

            {/* Expression Evaluation */}
            <div className="evaluate-section">
              <div className="evaluate-input">
                <input
                  type="text"
                  placeholder="Enter expression to evaluate..."
                  onKeyDown={(e) => {
                    if (e.key === 'Enter' && e.currentTarget.value) {
                      handleEvaluate(e.currentTarget.value);
                      e.currentTarget.value = '';
                    }
                  }}
                  disabled={executing}
                />
                <button
                  className="evaluate-button"
                  onClick={() => {
                    const input = document.querySelector('.evaluate-input input') as HTMLInputElement;
                    if (input && input.value) {
                      handleEvaluate(input.value);
                      input.value = '';
                    }
                  }}
                  disabled={executing}
                >
                  Evaluate
                </button>
              </div>
            </div>

            {/* Execution status */}
            {executionMessage && (
              <div className="execution-status">
                <div className="status-message">{executionMessage}</div>
              </div>
            )}

            {/* Source code view */}
            <div className="debug-panel-section">
              <div className="section-header">
                <h4>
                  <button 
                    className="toggle-button" 
                    onClick={() => setSourceCodeSectionOpen(!sourceCodeSectionOpen)}
                    aria-label={sourceCodeSectionOpen ? "Collapse section" : "Expand section"}
                  >
                    {sourceCodeSectionOpen ? "▼" : "►"}
                  </button>
                  Source Code
                </h4>
                <div className="section-actions">
                  <span className="source-code-help">Click on line numbers to set/clear breakpoints</span>
                  <button 
                    className="refresh-button"
                    onClick={fetchSourceCode}
                    disabled={executing}
                    title="Refresh Source Code"
                  >
                    🔄
                  </button>
                </div>
              </div>
              
              {sourceCodeSectionOpen && (
                <div className="source-code-view" ref={sourceCodeRef}>
                  {sourceCode.length === 0 ? (
                    <div className="empty-message">No source code available</div>
                  ) : (
                    <pre className="code-container">
                      {sourceCode.map((line) => (
                        <div 
                          key={line.lineNumber} 
                          className={`code-line ${line.lineNumber === currentLine ? 'current-line' : ''} ${line.isBreakpoint ? 'breakpoint-line' : ''}`}
                          data-line={line.lineNumber}
                        >
                          <span 
                            className="line-number" 
                            onClick={() => {
                              if (selectedBreakpoint && selectedBreakpoint.taskId) {
                                toggleLineBreakpoint(line.lineNumber);
                              }
                            }}
                            title={line.isBreakpoint ? "Click to remove breakpoint" : "Click to set breakpoint"}
                          >
                            {line.isBreakpoint && <span className="breakpoint-indicator">●</span>}
                            {line.lineNumber}
                          </span>
                          <span className="line-content">
                            {line.content}
                          </span>
                        </div>
                      ))}
                    </pre>
                  )}
                </div>
              )}
            </div>

            {/* Debug Output Panel (replaces terminal) */}
            <div className="debug-panel-section">
              <div className="section-header">
                <h4>
                  <button 
                    className="toggle-button" 
                    onClick={() => setOutputSectionOpen(!outputSectionOpen)}
                    aria-label={outputSectionOpen ? "Collapse section" : "Expand section"}
                  >
                    {outputSectionOpen ? "▼" : "►"}
                  </button>
                  Eval Results
                </h4>
                <button 
                  className="clear-button"
                  onClick={() => setOutput('')}
                  disabled={executing}
                  title="Clear Results"
                >
                  Clear
                </button>
              </div>
              
              {outputSectionOpen && (
                <div className="debug-output-view" ref={outputRef}>
                  <pre className="output-container">
                    {output}
                  </pre>
                </div>
              )}
            </div>

            {/* Stack frames */}
            <div className="debug-panel-section">
              <div className="section-header">
                <h4>
                  <button 
                    className="toggle-button" 
                    onClick={() => setStackSectionOpen(!stackSectionOpen)}
                    aria-label={stackSectionOpen ? "Collapse section" : "Expand section"}
                  >
                    {stackSectionOpen ? "▼" : "►"}
                  </button>
                  Call Stack
                </h4>
              </div>
              
              {stackSectionOpen && (
                <div className="stack-view">
                  {stackFrames.length === 0 ? (
                    <div className="empty-message">No call stack available</div>
                  ) : (
                    <ul className="stack-list">
                      {stackFrames.map((frame, index) => (
                        <li 
                          key={index} 
                          className={`stack-frame ${currentFile === frame.filename && currentLine === frame.lineNumber ? 'current-frame' : ''}`}
                          onClick={() => executeDebugCommand(`frame ${index}`)}
                        >
                          <span className="frame-function">{frame.functionName}</span>
                          <span className="frame-location">
                            {frame.filename}:{frame.lineNumber}
                          </span>
                        </li>
                      ))}
                    </ul>
                  )}
                </div>
              )}
            </div>
          </>
        )}
        
        {/* No active breakpoint message */}
        {(!selectedBreakpoint || !selectedBreakpoint.taskId) && (
          <div className="no-debug-session">
            <div className="empty-message">
              <p>No active debug session. Set a breakpoint first.</p>
              <p>Debugging will start when a Ray task hits your breakpoint.</p>
            </div>
          </div>
        )}

      </>
    );
  };

  return (
    <div className={isTab ? "debug-panel-as-tab" : "debug-panel"}>
      {renderContent()}
      <style>{`
        .source-code-view {
          max-height: 300px;
          overflow: auto;
          background-color: #1e1e1e;
          border-radius: 4px;
          color: #f0f0f0;
          tab-size: 4;
          -moz-tab-size: 4;
          -o-tab-size: 4;
          font-variant-ligatures: none;
        }
        
        .debug-output-view {
          max-height: 200px;
          overflow: auto;
          background-color: #1e1e1e;
          border-radius: 4px;
          color: #f0f0f0;
          margin-top: 5px;
          padding: 8px;
        }

        .output-container {
          font-family: monospace;
          margin: 0;
          padding: 0;
          white-space: pre-wrap;
          font-size: 13px;
          line-height: 1.4;
        }
        
        .evaluate-section {
          margin: 12px 0;
        }
        
        .evaluate-input {
          display: flex;
          gap: 8px;
        }
        
        .evaluate-input input {
          flex: 1;
          padding: 6px 10px;
          border: 1px solid #ddd;
          border-radius: 4px;
          font-family: monospace;
        }
        
        .evaluate-button {
          padding: 6px 12px;
          background-color: #f0f0f0;
          border: 1px solid #ddd;
          border-radius: 4px;
          cursor: pointer;
        }

        .clear-button {
          padding: 2px 6px;
          background-color: #f0f0f0;
          border: 1px solid #ddd;
          border-radius: 4px;
          cursor: pointer;
          font-size: 12px;
        }
        
        .evaluate-button:hover, .clear-button:hover {
          background-color: #e0e0e0;
        }
        
        .code-container {
          font-family: 'SFMono-Regular', Consolas, 'Liberation Mono', Menlo, monospace;
          margin: 0;
          padding: 0;
          white-space: pre;
          tab-size: 4;
          -moz-tab-size: 4;
          -o-tab-size: 4;
          font-variant-ligatures: none;
        }
        
        .code-line {
          display: flex;
          padding: 0 8px;
          white-space: pre;
          line-height: 1.5;
          font-variant-ligatures: none;
        }
        
        .current-line {
          background-color: #2d4151;
        }
        
        .breakpoint-line {
          background-color: rgba(255, 0, 0, 0.2);
        }

        .breakpoint-line.current-line {
          background-color: rgba(255, 0, 0, 0.3);
        }
        
        .line-number {
          opacity: 0.6;
          text-align: right;
          padding-right: 12px;
          user-select: none;
          width: 40px;
          cursor: pointer;
          position: relative;
        }
        
        .line-number:hover {
          opacity: 1;
          color: #ffffff;
        }
        
        .breakpoint-indicator {
          color: #ff5555;
          position: absolute;
          left: -8px;
          top: 1px;
        }
        
        .line-content {
          flex: 1;
          white-space: pre;
          tab-size: 4;
          -moz-tab-size: 4;
          -o-tab-size: 4;
          font-family: 'SFMono-Regular', Consolas, 'Liberation Mono', Menlo, monospace;
          font-variant-ligatures: none;
          unicode-bidi: embed;
        }
        
        .current-marker {
          color: #00ffff;
          font-weight: bold;
        }
                
        .stack-view {
          max-height: 150px;
          overflow: auto;
        }
        
        .stack-list {
          list-style: none;
          padding: 0;
          margin: 0;
        }
        
        .stack-frame {
          padding: 8px;
          cursor: pointer;
          display: flex;
          justify-content: space-between;
          border-bottom: 1px solid #ddd;
        }
        
        .stack-frame:hover {
          background-color: #f5f5f5;
        }
        
        .current-frame {
          background-color: #e6f7ff;
          font-weight: bold;
        }
        
        .frame-function {
          font-weight: bold;
        }
        
        .frame-location {
          color: #666;
          font-size: 0.9em;
        }
        
        .debug-controls {
          display: flex;
          gap: 8px;
          margin: 12px 0;
          flex-wrap: wrap;
        }
        
        .debug-button {
          padding: 6px 12px;
          background-color: #f0f0f0;
          border: 1px solid #ddd;
          border-radius: 4px;
          cursor: pointer;
          font-size: 13px;
        }
        
        .debug-button:hover {
          background-color: #e0e0e0;
        }
        
        .debug-button:disabled {
          opacity: 0.6;
          cursor: not-allowed;
        }
        
        .execution-status {
          padding: 8px;
          margin: 8px 0;
          background-color: #f0f7ff;
          border-radius: 4px;
          border-left: 4px solid #1890ff;
        }
        
        .no-debug-session {
          margin: 20px 0;
          padding: 20px;
          background-color: #f9f9f9;
          border-radius: 4px;
          text-align: center;
        }

        .section-actions {
          display: flex;
          align-items: center;
          gap: 8px;
        }

        .source-code-help {
          font-size: 12px;
          color: #777;
          font-style: italic;
        }
      `}</style>
    </div>
  );
};

export default DebugPanel;