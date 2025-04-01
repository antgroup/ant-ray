import React, { useState, useEffect, useRef } from "react";
import { Breakpoint, getBreakpoints, setBreakpoint, sendDebugCommand, getDebugOutput, DebugOutputEntry, closeDebugSession } from "../../service/debug-insight";
import { Terminal } from "@xterm/xterm";
import "@xterm/xterm/css/xterm.css";
import "./RayVisualization.css";

type DebugPanelProps = {
  open: boolean;
  onClose?: () => void;
  jobId?: string;
  isTab?: boolean;
}

const DebugPanel: React.FC<DebugPanelProps> = ({ open, onClose, jobId, isTab = false }) => {
  const [breakpoints, setBreakpoints] = useState<Breakpoint[]>([]);
  const [actorClass, setActorClass] = useState("");
  const [actorName, setActorName] = useState("");
  const [methodName, setMethodName] = useState("");
  const [functionName, setFunctionName] = useState("");
  const [commandHistory, setCommandHistory] = useState<Array<{cmd: string, response: string}>>([]);
  const [command, setCommand] = useState("");
  const [currentTaskId, setCurrentTaskId] = useState<string | null>(null);
  const [refreshInterval, setRefreshInterval] = useState<NodeJS.Timeout | null>(null);
  const [outputRefreshInterval, setOutputRefreshInterval] = useState<NodeJS.Timeout | null>(null);
  const [autoRefresh, setAutoRefresh] = useState(false);
  const [autoRefreshOutput, setAutoRefreshOutput] = useState(true);
  const [isLoading, setIsLoading] = useState(false);
  const commandHistoryRef = useRef<HTMLDivElement>(null);
  const terminalRef = useRef<HTMLDivElement>(null);
  const xtermRef = useRef<Terminal | null>(null);
  const commandInputRef = useRef<HTMLInputElement>(null);
  const [selectedBreakpoint, setSelectedBreakpoint] = useState<Breakpoint | null>(null);
  const [commandHistoryIndex, setCommandHistoryIndex] = useState(-1);
  const [commandBuffer, setCommandBuffer] = useState("");
  const [commandsForNav, setCommandsForNav] = useState<string[]>([]);
  const [isDisablingBreakpoint, setIsDisablingBreakpoint] = useState(false);

  // Add state for foldable sections
  const [breakpointSectionOpen, setBreakpointSectionOpen] = useState(true);
  const [breakpointListOpen, setBreakpointListOpen] = useState(true);
  const [consoleSectionOpen, setConsoleSectionOpen] = useState(true);
  
  const [isClosingSession, setIsClosingSession] = useState(false);
  
  useEffect(() => {
    if (open) {
      fetchBreakpoints();
    }
    
    return () => {
      if (refreshInterval) {
        clearInterval(refreshInterval);
      }
      if (outputRefreshInterval) {
        clearInterval(outputRefreshInterval);
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
  
  // Only auto-refresh output for the currently selected breakpoint/task
  useEffect(() => {
    if (autoRefreshOutput && selectedBreakpoint && selectedBreakpoint.taskId === currentTaskId) {
      const interval = setInterval(() => {
        fetchDebugOutput();
      }, 2000);
      setOutputRefreshInterval(interval);
    } else if (outputRefreshInterval) {
      clearInterval(outputRefreshInterval);
      setOutputRefreshInterval(null);
    }
    
    return () => {
      if (outputRefreshInterval) {
        clearInterval(outputRefreshInterval);
      }
    };
  }, [autoRefreshOutput, currentTaskId, selectedBreakpoint]);
  
  // When selected breakpoint changes, fetch the debug output
  useEffect(() => {
    if (selectedBreakpoint && selectedBreakpoint.taskId) {
      fetchDebugOutput();
    }
  }, [selectedBreakpoint]);
  
  // Auto scroll to bottom when command history updates
  useEffect(() => {
    if (commandHistoryRef.current) {
      commandHistoryRef.current.scrollTop = commandHistoryRef.current.scrollHeight;
    }
  }, [commandHistory]);

  // Focus on input when shell is active
  useEffect(() => {
    if (selectedBreakpoint && commandInputRef.current) {
      commandInputRef.current.focus();
    }
  }, [selectedBreakpoint]);

  // Store commands for navigation history
  useEffect(() => {
    // Extract commands from the full history for navigation
    const cmdHistory = commandHistory
      .filter(entry => entry.cmd)
      .map(entry => entry.cmd);
    
    // Ensure no duplicates and maintain order
    const uniqueCmds: string[] = [];
    for (let i = cmdHistory.length - 1; i >= 0; i--) {
      if (!uniqueCmds.includes(cmdHistory[i])) {
        uniqueCmds.unshift(cmdHistory[i]);
      }
    }
    
    // Limit to 50 commands max
    setCommandsForNav(uniqueCmds.slice(-50));
  }, [commandHistory]);

  // Initialize xterm when terminal container is mounted or a breakpoint is selected
  useEffect(() => {
    if (consoleSectionOpen && terminalRef.current && selectedBreakpoint) {
      // Clean up any existing terminal
      if (xtermRef.current) {
        xtermRef.current.dispose();
      }
      
      // Create new terminal
      const term = new Terminal({
        fontFamily: 'monospace',
        fontSize: 14,
        theme: {
          background: '#1e1e1e',
          foreground: '#f0f0f0',
          cursor: '#f0f0f0',
        },
        cursorBlink: true,
        scrollback: 1000,
        disableStdin: false,
        convertEol: true,
      });
      
      // Apply full container size to terminal
      term.onResize(() => {
        // Terminal resized
        console.log('Terminal resized');
      });
      
      term.open(terminalRef.current);
      
      // Apply custom CSS to hide scrollbars
      if (terminalRef.current) {
        // Find and modify xterm-viewport
        const viewports = terminalRef.current.querySelectorAll('.xterm-viewport');
        viewports.forEach(viewport => {
          if (viewport instanceof HTMLElement) {
            viewport.style.overflowY = 'auto'; // Allow vertical scrolling
            viewport.style.overflowX = 'auto'; // Allow horizontal scrolling
            viewport.style['scrollbarWidth' as any] = 'thin'; // Show thin scrollbar for Firefox
            viewport.style['msOverflowStyle' as any] = '-ms-autohiding-scrollbar'; // Show scrollbar for IE/Edge
          }
        });
      }
      
      // Set up terminal
      term.writeln('Debug shell connected. Type PDB commands below.');
      term.writeln('Common PDB commands: n (next), s (step), c (continue), q (quit)');
      term.writeln('Use the up/down arrow keys to navigate command history.');
      term.writeln('');
      term.write('(Pdb) ');
      
      // Scroll to bottom after initial text is written
      scrollToBottom(term);
      
      // Auto scroll to bottom when terminal content changes
      term.onData(() => {
        scrollToBottom(term);
      });
      
      // Current command text buffer
      let currentCommand = '';
      let historyPosition = -1;
      
      // Set up input handling
      term.onKey(({ key, domEvent }) => {
        const printable = !domEvent.altKey && !domEvent.ctrlKey && !domEvent.metaKey;
        
        if (domEvent.key === 'Enter') {
          // Get current line
          const line = term.buffer.active.getLine(term.buffer.active.cursorY)?.translateToString() || '';
          const promptIndex = line.lastIndexOf('(Pdb) ');
          const cmd = promptIndex >= 0 ? line.substring(promptIndex + 6).trim() : currentCommand.trim();
          
          // Reset current command
          currentCommand = '';
          historyPosition = -1;
          
          // Handle command
          if (cmd) {
            // Write a new line
            term.writeln('');
            
            // Handle quit command separately
            if (cmd === 'q' || cmd === 'quit') {
              term.writeln("Error: 'q' and 'quit' commands are disabled. Please use the 'Close Session' button instead.");
              term.write('(Pdb) ');
              
              // Scroll to bottom
              scrollToBottom(term);
              return;
            }
            
            // Execute the command
            executePdbCommand(cmd, term);
          } else {
            // Empty command, just add a new prompt
            term.writeln('');
            term.write('(Pdb) ');
            
            // Scroll to bottom
            scrollToBottom(term);
          }
        } else if (domEvent.key === 'ArrowUp') {
          // Navigate up in history
          if (historyPosition === -1 && commandsForNav.length > 0) {
            // Save current input if we're just starting to navigate
            currentCommand = getPromptInput(term);
          }
          
          if (commandsForNav.length > 0) {
            const nextPosition = historyPosition === -1 ? 
              commandsForNav.length - 1 : 
              Math.max(0, historyPosition - 1);
              
            // Only update if position changed
            if (nextPosition !== historyPosition) {
              historyPosition = nextPosition;
              const historyCommand = commandsForNav[historyPosition];
              clearCurrentPrompt(term);
              term.write('(Pdb) ' + historyCommand);
            }
          }
        } else if (domEvent.key === 'ArrowDown') {
          // Navigate down in history
          if (historyPosition >= 0) {
            if (historyPosition < commandsForNav.length - 1) {
              // Move to next command in history
              historyPosition++;
              const historyCommand = commandsForNav[historyPosition];
              clearCurrentPrompt(term);
              term.write('(Pdb) ' + historyCommand);
            } else if (historyPosition === commandsForNav.length - 1) {
              // Return to original input
              historyPosition = -1;
              clearCurrentPrompt(term);
              term.write('(Pdb) ' + currentCommand);
            }
          }
        } else if (domEvent.key === 'Backspace') {
          // Handle backspace - only delete if there's text after prompt
          const input = getPromptInput(term);
          if (input.length > 0) {
            // Move cursor back, write a space to erase the character, then move back again
            term.write('\b \b');
            // Update current command
            currentCommand = input.slice(0, -1);
          }
        } else if (printable) {
          // Print the character
          term.write(key);
          // Update current command
          currentCommand += key;
        }
      });
      
      // Store the terminal instance
      xtermRef.current = term;
      
      // Fetch initial output
      fetchDebugOutput().then(output => {
        if (output) {
          term.writeln(output);
          term.write('(Pdb) ');
          
          // Scroll to bottom after output is loaded
          scrollToBottom(term);
        }
      });
    }
    
    return () => {
      // Clean up terminal on unmount
      if (xtermRef.current) {
        xtermRef.current.dispose();
        xtermRef.current = null;
      }
    };
  }, [consoleSectionOpen, selectedBreakpoint]);
  
  // Helper to get input text after the prompt
  const getPromptInput = (term: Terminal): string => {
    const line = term.buffer.active.getLine(term.buffer.active.cursorY)?.translateToString() || '';
    const promptIndex = line.lastIndexOf('(Pdb) ');
    return promptIndex >= 0 ? line.substring(promptIndex + 6) : '';
  };
  
  // Helper to clear the current prompt and input
  const clearCurrentPrompt = (term: Terminal) => {
    const input = getPromptInput(term);
    
    // Move cursor to the beginning of input
    for (let i = 0; i < input.length; i++) {
      term.write('\b \b');
    }
  };

  // Execute PDB command and handle the response
  const executePdbCommand = async (cmd: string, term: Terminal) => {
    if (!cmd.trim() || !jobId || !currentTaskId || !selectedBreakpoint) return;
    
    try {
      setIsLoading(true);
      
      const response = await sendDebugCommand({
        job_id: jobId,
        task_id: currentTaskId,
        cmd: cmd + '\n'  // Add newline for PDB
      });
      
      console.log("Debug command response:", response);
      
      // Add command to history for navigation
      setCommandsForNav(prev => {
        const newCommands = [...prev];
        if (!newCommands.includes(cmd)) {
          newCommands.push(cmd);
          // Limit history size
          if (newCommands.length > 50) {
            newCommands.shift();
          }
        }
        return newCommands;
      });
      
      // Write response to terminal
      if (response && term) {
        term.writeln(response);
        term.write('(Pdb) ');
        
        // Scroll to bottom
        scrollToBottom(term);
      }
      
    } catch (error) {
      console.error("Failed to send debug command:", error);
      if (term) {
        term.writeln("Error: Failed to execute command");
        term.write('(Pdb) ');
        
        // Scroll to bottom
        scrollToBottom(term);
      }
    } finally {
      setIsLoading(false);
    }
  };

  // Helper function to scroll terminal to bottom
  const scrollToBottom = (term: Terminal) => {
    try {
      // Use setTimeout to ensure scroll happens after render
      setTimeout(() => {
        if (term && terminalRef.current) {
          const viewport = terminalRef.current.querySelector('.xterm-viewport');
          if (viewport instanceof HTMLElement) {
            viewport.scrollTop = viewport.scrollHeight;
          }
        }
      }, 0);
    } catch (error) {
      console.error("Failed to scroll to bottom:", error);
    }
  };

  const fetchDebugOutput = async () => {
    if (!selectedBreakpoint || !selectedBreakpoint.taskId) return null;
    
    try {
      const output = await getDebugOutput(selectedBreakpoint.taskId);
      if (output && typeof output === 'string' && output.trim()) {
        // If we have an xterm instance, write the output there
        if (xtermRef.current) {
          xtermRef.current.writeln(output);
          xtermRef.current.write('(Pdb) ');
          
          // Scroll to bottom after new output is displayed
          scrollToBottom(xtermRef.current);
        }
        return output;
      }
    } catch (error) {
      console.error("Failed to fetch debug output:", error);
    }
    return null;
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
          // Reset command history when selecting a new breakpoint
          setCommandHistory([]);
          // Reset command history navigation
          setCommandHistoryIndex(-1);
          // Reset command buffer
          setCommandBuffer("");
        }
      } else {
        // No active breakpoints, clear current task ID
        setCurrentTaskId(null);
        // If there was a selected breakpoint but it's no longer active, clear it
        if (selectedBreakpoint && selectedBreakpoint.taskId) {
          setSelectedBreakpoint(null);
          // Clear command history when deselecting a breakpoint
          setCommandHistory([]);
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
        // For active breakpoints, we only want to clear the selection if it was successfully disabled
        // For non-active breakpoints, just refresh the list
        if (bp.taskId && bp.taskId !== 'unknown' && selectedBreakpoint && selectedBreakpoint.taskId === bp.taskId) {
          // Add message to command history that breakpoint was disabled
          setCommandHistory(prev => [...prev, { 
            cmd: "", 
            response: "Breakpoint disabled successfully." 
          }]);
          
          // Close the debug session for active breakpoints
          await closeDebugSession(jobId!, bp.taskId);
          
          // Clear the selection after session is closed
          setSelectedBreakpoint(null);
          setCurrentTaskId(null);
        }
        
        // Refresh breakpoints list
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
          
          // Add message to command history that session was closed
          setCommandHistory(prev => [...prev, { 
            cmd: "", 
            response: "Debug session closed successfully." 
          }]);
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
      // Only clear history if selecting a different breakpoint
      if (!selectedBreakpoint || selectedBreakpoint.taskId !== bp.taskId) {
        setSelectedBreakpoint(bp);
        setCurrentTaskId(bp.taskId);
        
        // Clear command history when switching to a different breakpoint
        setCommandHistory([]);
        
        // Reset command history navigation
        setCommandHistoryIndex(-1);
        
        // Reset command buffer
        setCommandBuffer("");
      }
    }
  };

  const handleSendCommand = async () => {
    if (!command.trim() || !jobId || !currentTaskId || !selectedBreakpoint) return;
    
    const cmd = command.trim();
    
    // Prevent dangerous PDB commands like 'q' that can crash the debug session
    if (cmd === 'q' || cmd === 'quit') {
      if (xtermRef.current) {
        xtermRef.current.writeln("Error: 'q' and 'quit' commands are disabled. Please use the 'Close Session' button instead.");
        xtermRef.current.write('(Pdb) ');
      }
      setCommand("");
      return;
    }
    
    setCommand("");
    
    // Get the current terminal
    const term = xtermRef.current;
    if (!term) return;
    
    try {
      setIsLoading(true);
      
      const response = await sendDebugCommand({
        job_id: jobId,
        task_id: currentTaskId,
        cmd: cmd + '\n'  // Add newline for PDB
      });
      
      console.log("Debug command response:", response);
      
      // Add command to history for navigation
      setCommandsForNav(prev => {
        const newCommands = [...prev];
        if (!newCommands.includes(cmd)) {
          newCommands.push(cmd);
          // Limit history size
          if (newCommands.length > 50) {
            newCommands.shift();
          }
        }
        return newCommands;
      });
      
      // Write response to terminal
      if (response && term) {
        term.writeln(response);
        term.write('(Pdb) ');
      }
      
    } catch (error) {
      console.error("Failed to send debug command:", error);
      if (term) {
        term.writeln("Error: Failed to execute command");
        term.write('(Pdb) ');
      }
    } finally {
      setIsLoading(false);
    }
  };

  const handleKeyDown = (e: React.KeyboardEvent<HTMLInputElement>) => {
    if (e.key === 'Enter') {
      e.preventDefault();
      handleSendCommand();
    } else if (e.key === 'ArrowUp') {
      e.preventDefault();
      
      // Navigate command history upward
      if (commandsForNav.length > 0) {
        // If we're starting navigation, save current input
        if (commandHistoryIndex === -1) {
          setCommandBuffer(command);
        }
        
        const newIndex = commandHistoryIndex === -1 ? 
          commandsForNav.length - 1 : Math.max(0, commandHistoryIndex - 1);
          
        setCommandHistoryIndex(newIndex);
        setCommand(commandsForNav[newIndex]);
      }
    } else if (e.key === 'ArrowDown') {
      e.preventDefault();
      
      // Navigate command history downward
      if (commandHistoryIndex >= 0) {
        const newIndex = commandHistoryIndex + 1;
        
        if (newIndex >= commandsForNav.length) {
          // Return to buffer at the end of history
          setCommandHistoryIndex(-1);
          setCommand(commandBuffer);
        } else {
          setCommandHistoryIndex(newIndex);
          setCommand(commandsForNav[newIndex]);
        }
      }
    } else if (e.key === 'Tab') {
      e.preventDefault();
      // Future: Could implement tab completion here
    }
  };

  // Add validation for command input to prevent 'q'/'quit'
  const handleCommandChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    const value = e.target.value;
    // If user tries to type just 'q' or 'quit', show a warning and don't update the input
    if (value === 'q' || value === 'quit') {
      // We could show a tooltip or warning here
      return; // Don't update the command value
    }
    setCommand(value);
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
                onClick={() => setConsoleSectionOpen(!consoleSectionOpen)}
                aria-label={consoleSectionOpen ? "Collapse section" : "Expand section"}
              >
                {consoleSectionOpen ? "▼" : "►"}
              </button>
              Debug Shell
            </h4>
            <div className="auto-refresh">
              <button 
                className="refresh-button"
                onClick={fetchDebugOutput}
                disabled={isLoading || !selectedBreakpoint}
                title="Refresh Output"
              >
                🔄
              </button>
              <label htmlFor="auto-refresh-output">Auto</label>
              <input
                id="auto-refresh-output"
                type="checkbox"
                checked={autoRefreshOutput}
                onChange={(e: React.ChangeEvent<HTMLInputElement>) => setAutoRefreshOutput(e.target.checked)}
                disabled={isLoading || !selectedBreakpoint}
              />
            </div>
          </div>

        </div>
          {consoleSectionOpen && (
            <div className="terminal-container">
              {selectedBreakpoint ? (
                <div ref={terminalRef} className="xterm-container"></div>
              ) : (
                  <div className="empty-message">
                    <div className="terminal-welcome">
                    No active breakpoint selected. Set a breakpoint first.
                      </div>
                      </div>
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
        
        {currentTaskId && (
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
            </div>
          </div>
        )}
        

      </>
    );
  };

  return (
    <div className={isTab ? "debug-panel-as-tab" : "debug-panel"}>
      {renderContent()}
    </div>
  );
};

export default DebugPanel;