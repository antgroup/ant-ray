import React, { useRef, useState } from "react";
import { Prism as SyntaxHighlighter } from "react-syntax-highlighter";
import { vscDarkPlus } from "react-syntax-highlighter/dist/esm/styles/prism";
import {
  Breakpoint,
  closeDebugSession,
  getBreakpoints,
  getDebugState,
  sendDebugCommand,
  setBreakpoint,
  switchDebugMode,
} from "../../service/debug-insight";
import { Node } from "./InfoCard";
import "./RayVisualization.css";

type DebugPanelProps = {
  open: boolean;
  data: Node | null;
  onClose?: () => void;
  jobId?: string;
  isTab?: boolean;
  setActiveDebugSession?: (data: Breakpoint) => void;
};

type SourceCodeLine = {
  lineNumber: number;
  content: string;
  isBreakpoint?: boolean;
};

// Add new types for session storage
type SessionState = {
  currentLine: number | null;
  sourceCode: SourceCodeLine[];
  currentFile: string | null;
};

type SessionStorage = {
  [taskId: string]: SessionState;
};

const DebugPanel: React.FC<DebugPanelProps> = ({
  open,
  onClose,
  jobId,
  isTab = false,
  data,
  setActiveDebugSession,
}) => {
  const [breakpoints, setBreakpoints] = useState<Breakpoint[]>([]);
  const [actorClass, setActorClass] = useState("");
  const [actorName, setActorName] = useState("");
  const [methodName, setMethodName] = useState("");
  const [functionName, setFunctionName] = useState("");
  const [isActorMode, setIsActorMode] = useState(true);
  const [debugMode, setDebugMode] = useState(false);
  const [currentTaskId, setCurrentTaskId] = useState<string | null>(null);
  const [refreshInterval, setRefreshInterval] =
    useState<NodeJS.Timeout | null>(null);
  const [isLoading, setIsLoading] = useState(false);
  const [selectedBreakpoint, setSelectedBreakpoint] =
    useState<Breakpoint | null>(null);
  const [isDisablingBreakpoint, setIsDisablingBreakpoint] = useState(false);

  // Initialize debug mode state
  React.useEffect(() => {
    if (jobId) {
      const initDebugMode = async () => {
        try {
          const state = await getDebugState(jobId);
          setDebugMode(state);
        } catch (error) {
          console.error("Failed to get initial debug state:", error);
        }
      };
      initDebugMode();
    }
  }, [jobId]);

  // New state for IDE-like debug interface
  const [sourceCode, setSourceCode] = useState<SourceCodeLine[]>([]);
  const [currentFile, setCurrentFile] = useState<string | null>(null);
  const [executing, setExecuting] = useState(false);
  const [executionMessage, setExecutionMessage] = useState<string | null>(null);
  const [output, setOutput] = useState<string>("");
  const [isFetchingBreakpoints, setIsFetchingBreakpoints] = useState(false);
  const [isParsingOutput, setIsParsingOutput] = useState(false);

  // Add state for foldable sections
  const [breakpointSectionOpen, setBreakpointSectionOpen] = useState(true);
  const [breakpointListOpen, setBreakpointListOpen] = useState(true);
  const [sourceCodeSectionOpen, setSourceCodeSectionOpen] = useState(true);
  const [outputSectionOpen, setOutputSectionOpen] = useState(true);

  // State to store mapping from 'filename:lineNumber' to breakpoint index
  const [lineBreakpoints, setLineBreakpoints] = useState<Map<string, number>>(
    new Map(),
  );

  const sourceCodeRef = useRef<HTMLDivElement>(null);
  const outputRef = useRef<HTMLDivElement>(null);

  // Add reference to track last command time
  const lastCommandTimeRef = useRef<number>(0);
  const COMMAND_COOLDOWN_MS = 1000; // Min time between commands

  // Simple flags to track operations in progress
  const isSourceCodeFetchingRef = useRef<boolean>(false);
  const isBreakpointsFetchingRef = useRef<boolean>(false);

  // Add a state to track invalid sessions
  const [invalidSessions, setInvalidSessions] = useState<Set<string>>(
    new Set(),
  );
  const [currentLine, setCurrentLine] = useState<number | null>(null);

  // Add a state for long polling
  const [isLongPolling, setIsLongPolling] = useState(false);

  // Add a reference for the code content
  const codeContentRef = useRef<HTMLDivElement>(null);

  // Add new state for session storage
  const [sessionStates, setSessionStates] = useState<SessionStorage>({});

  // Add local storage functions
  const saveSessionState = (taskId: string) => {
    if (!taskId) {
      return;
    }

    const state: SessionState = {
      currentLine,
      sourceCode,
      currentFile,
    };

    setSessionStates((prev) => ({
      ...prev,
      [taskId]: state,
    }));

    // Save to local storage
    try {
      const storageKey = `debug_session_${jobId}_${taskId}`;
      localStorage.setItem(storageKey, JSON.stringify(state));
    } catch (error) {
      console.error("Failed to save session state to local storage:", error);
    }
  };

  const loadSessionState = (taskId: string): SessionState | null => {
    if (!taskId) {
      return null;
    }

    // First check in-memory state
    if (sessionStates[taskId]) {
      return sessionStates[taskId];
    }

    // Then check local storage
    try {
      const storageKey = `debug_session_${jobId}_${taskId}`;
      const stored = localStorage.getItem(storageKey);
      if (stored) {
        const state = JSON.parse(stored) as SessionState;
        // Update in-memory state
        setSessionStates((prev) => ({
          ...prev,
          [taskId]: state,
        }));
        return state;
      }
    } catch (error) {
      console.error("Failed to load session state from local storage:", error);
    }

    return null;
  };

  // Function to scroll to the current line
  const scrollToCurrentLine = () => {
    if (sourceCodeRef.current && currentLine) {
      const lineElement = sourceCodeRef.current.querySelector(
        `[data-line-number="${currentLine}"]`,
      );
      if (lineElement) {
        lineElement.scrollIntoView({
          behavior: "smooth",
          block: "center",
        });
      }
    }
  };

  // Update the line markers scroll position to match the code content
  const synchronizeScrolling = () => {
    if (codeContentRef.current && sourceCodeRef.current) {
      const lineMarkers = sourceCodeRef.current.querySelector(".line-markers");
      if (lineMarkers) {
        lineMarkers.scrollTop = codeContentRef.current.scrollTop;
      }
    }
  };

  // Add a scroll listener to the code content
  React.useEffect(() => {
    const codeContent = codeContentRef.current;
    if (codeContent) {
      const handleScroll = () => synchronizeScrolling();
      codeContent.addEventListener("scroll", handleScroll);
      return () => codeContent.removeEventListener("scroll", handleScroll);
    }
  }, []);

  React.useEffect(() => {
    console.log("Debug panel data", data);
    if (data) {
      // Find matching breakpoint based on node type and data
      console.log("selecting breakpoint", breakpoints);
      const matchingBreakpoint = breakpoints.find((bp) => {
        if (!bp.enable) {
          return false;
        }

        switch (data.type) {
          case "method":
            // For methods, match both actor ID and method name
            return bp.bpActorId === data.actorId && bp.methodName === data.name;

          case "function":
            // For functions, match the function name
            return bp.funcName === data.name;

          default:
            return false;
        }
      });

      if (matchingBreakpoint) {
        handleSelectBreakpoint(matchingBreakpoint);
      }
    }
    // eslint-disable-next-line
  }, [data, breakpoints]);

  // Update to scroll to current line when it changes
  React.useEffect(() => {
    scrollToCurrentLine();
    // eslint-disable-next-line
  }, [currentLine]);

  // Function to check if we should fetch breakpoints
  const shouldFetchBreakpoints = () => {
    // Avoid fetching breakpoints if:
    // 1. We're already fetching them
    // 2. We're in long polling mode
    // 3. We're executing another command
    return !isFetchingBreakpoints && !isLongPolling && !executing;
  };

  // Function to check if we should fetch source code
  const shouldFetchSourceCode = () => {
    return !isSourceCodeFetchingRef.current && !isLongPolling && !executing;
  };

  // Function to initialize the component when opened
  const initializeComponent = () => {
    if (open) {
      // Fetch breakpoints and ensure source code is loaded for the first active breakpoint
      fetchBreakpoints().then(() => {
        // If we already have a selected breakpoint but no source code, fetch it
        if (
          selectedBreakpoint &&
          selectedBreakpoint.taskId &&
          sourceCode.length === 0
        ) {
          console.log(
            "Debug panel opened with selected breakpoint but no source code, fetching source",
          );

          // Use the proper sequence for fetching source code
          handleBreakpointSelectionAndFetchSource();
        }
      });
    }
  };

  // Function to scroll output to bottom
  const scrollOutputToBottom = () => {
    if (outputRef.current) {
      outputRef.current.scrollTop = outputRef.current.scrollHeight;
    }
  };

  // Function to handle breakpoint selection and fetch source code
  const handleBreakpointSelectionAndFetchSource = () => {
    // Only proceed if we have a valid breakpoint
    if (selectedBreakpoint && selectedBreakpoint.taskId) {
      console.log(
        "Initializing source code fetch sequence for selected breakpoint",
      );

      // Force set flags to ensure we can proceed with source code fetch
      isSourceCodeFetchingRef.current = true;

      // Clear the output when switching breakpoints
      updateOutput("");

      // Use a sequence of commands to get the proper debug state
      const fetchDebugInfo = async () => {
        try {
          console.log("Getting debug session information...");

          // First, get current location with 'where'
          const whereResponse = await sendDebugCommand({
            job_id: jobId,
            task_id: selectedBreakpoint.taskId,
            cmd: "where\n",
          });

          if (whereResponse) {
            // This will set the current file and line
            await parseDebugOutput(whereResponse, "where");

            // Get the source code listing
            const listResponse = await sendDebugCommand({
              job_id: jobId,
              task_id: selectedBreakpoint.taskId,
              cmd: "ll\n",
            });

            if (listResponse) {
              // This will set the source code while preserving current line
              await parseDebugOutput(listResponse, "ll");
            }

            // Make sure the current line is visible
            if (sourceCodeRef.current && currentLine) {
              const lineElement = sourceCodeRef.current.querySelector(
                `[data-line="${currentLine}"]`,
              );
              if (lineElement) {
                lineElement.scrollIntoView({
                  behavior: "smooth",
                  block: "center",
                });
              }
            }

            // Get breakpoints
            await fetchAndRecordAllBreakpoints();
          }

          setExecutionMessage(null);
        } catch (error) {
          console.error("Error fetching debug information:", error);
          setTimeout(() => setExecutionMessage(null), 3000);
        } finally {
          isSourceCodeFetchingRef.current = false;
        }
      };

      // Use setTimeout to ensure state is settled before fetching source code
      setTimeout(() => {
        fetchDebugInfo().catch((err) => {
          console.error("Error in debug info fetch sequence:", err);
          isSourceCodeFetchingRef.current = false;
        });
      }, 100);
    }
  };

  // Function to handle initial load of breakpoints
  const handleInitialLoad = () => {
    if (
      open &&
      selectedBreakpoint &&
      selectedBreakpoint.taskId &&
      shouldFetchSourceCode()
    ) {
      console.log(
        "Debug panel opened with active breakpoint, ensuring source code is loaded",
      );

      // Use a delay to avoid state update collisions
      setTimeout(() => {
        // Use the complete fetch sequence to get both source code and breakpoints
        handleBreakpointSelectionAndFetchSource();
      }, 100);
    }
  };

  // Master initialization function to replace all useEffect calls
  const initializeAll = (() => {
    // Only run once when component mounts
    let hasInitialized = false;

    return () => {
      if (!hasInitialized && open) {
        hasInitialized = true;

        // Initialize component
        initializeComponent();

        return () => {
          // Clean up intervals
          if (refreshInterval) {
            clearInterval(refreshInterval);
          }

          hasInitialized = false;
        };
      }

      return undefined;
    };
  })();

  // The only Effect hook that remains - handles initialization and cleanup
  // eslint-disable-next-line
  React.useEffect(initializeAll, [open]);

  // Add a dedicated effect to fetch source code when a breakpoint is selected
  React.useEffect(() => {
    // Only run if the panel is open and we have a selected breakpoint
    if (open && selectedBreakpoint && selectedBreakpoint.taskId) {
      console.log(
        "Selected breakpoint effect triggered, checking if source code needs to be fetched",
      );

      // Only fetch source code if:
      // 1. We don't have any source code loaded yet
      // 2. We're not already fetching source code
      // 3. We're not executing a command (especially not a step/continue command)
      // 4. We're not in long polling mode
      if (
        sourceCode.length === 0 &&
        !isSourceCodeFetchingRef.current &&
        !executing &&
        !isLongPolling
      ) {
        console.log(
          "No source code loaded for selected breakpoint, fetching it now",
        );

        // Use the comprehensive source code fetch sequence
        handleBreakpointSelectionAndFetchSource();
      } else if (sourceCode.length > 0) {
        console.log("Source code already loaded, skipping fetch");
      } else if (executing || isLongPolling) {
        console.log(
          "Command execution or long polling in progress, skipping source code fetch",
        );
      }
    }
    // eslint-disable-next-line
  }, [open, selectedBreakpoint, jobId, sourceCode.length]);

  // Create a state update handler system to replace useLayoutEffect hooks

  // Keep track of previous state values to detect changes
  const prevStateRef = useRef({
    output: "",
    currentLine: null as number | null,
    sourceCode: [] as SourceCodeLine[],
    selectedBreakpointTaskId: null as string | null,
    sourceCodeLength: 0,
    currentFile: null as string | null,
    isLongPolling: false,
    executing: false,
  });

  // Function to update output and scroll
  const updateOutput = (text: string) => {
    setOutput(text);
    // Schedule scrolling after state update
    setTimeout(() => scrollOutputToBottom(), 0);
    // Update ref
    prevStateRef.current.output = text;
  };

  // Custom trim function that only removes spaces, not tabs
  const trimSpacesOnly = (str: string): string => {
    return str.replace(/^[ ]+|[ ]+$/g, "");
  };

  // Parse debug output to extract source code, variables, and stack
  const parseDebugOutput = async (output: string, cmd?: string) => {
    if (!output || isParsingOutput) {
      return;
    }

    try {
      setIsParsingOutput(true);

      // Track if we found any source code lines
      let foundSourceCode = false;
      let parsedLines: SourceCodeLine[] = [];
      let newCurrentLine: number | null = null;
      let newCurrentFile: string | null = null;

      // Direct string parsing without regex
      const lines = output.split("\n");
      let lastLocationLine = -1;

      // First pass: Find file location and current line
      for (let i = 0; i < lines.length; i++) {
        const line = trimSpacesOnly(lines[i]);

        // First, check for file location information which is often in the format:
        // > /path/to/file.py(line_number)function_name()
        if (line.startsWith(">") && line.includes("(") && line.includes(")")) {
          const arrowRemoved = line.substring(1).trim();
          const openParenIndex = arrowRemoved.indexOf("(");

          if (openParenIndex > 0) {
            const filename = arrowRemoved.substring(0, openParenIndex).trim();
            const closeParenIndex = arrowRemoved.indexOf(")", openParenIndex);

            if (closeParenIndex > openParenIndex) {
              const lineNumberStr = arrowRemoved.substring(
                openParenIndex + 1,
                closeParenIndex,
              );
              const lineNumber = parseInt(lineNumberStr, 10);

              if (!isNaN(lineNumber)) {
                newCurrentFile = filename;

                // IMPORTANT: Only update the current line for 'where' command
                // or step/continue commands, NOT for 'll' command
                if (
                  cmd === "where" ||
                  [
                    "n",
                    "next",
                    "s",
                    "step",
                    "r",
                    "return",
                    "c",
                    "continue",
                  ].includes(cmd || "")
                ) {
                  console.log(
                    `Setting current line to ${lineNumber} from '${cmd}' command`,
                  );
                  newCurrentLine = lineNumber;
                }

                lastLocationLine = i;
              }
            }
          }
          continue;
        }
      }

      // If we found a new file, update it immediately
      if (newCurrentFile) {
        setCurrentFile(newCurrentFile);
      }

      // Second pass: Process source code lines with breakpoint information
      for (let i = 0; i < lines.length; i++) {
        const line = trimSpacesOnly(lines[i]);

        // Special handling for source line immediately after file location in step command output
        if (i === lastLocationLine + 1 && line.startsWith("->")) {
          if (newCurrentLine !== null) {
            const sourceContent = trimSpacesOnly(line.substring(2));
            console.log(`Found source line after location: ${sourceContent}`);

            // Check if this line has a breakpoint
            const hasBreakpoint = Boolean(
              newCurrentFile &&
                lineBreakpoints.has(`${newCurrentFile}:${newCurrentLine}`),
            );

            parsedLines.push({
              lineNumber: newCurrentLine,
              content: sourceContent.substring(1),
              isBreakpoint: hasBreakpoint,
            });

            foundSourceCode = true;
            continue;
          }
        }

        // Then look for lines with format like "24  ->" or "24    " that indicate source code
        if (line.length > 0 && !isNaN(parseInt(line.charAt(0), 10))) {
          const firstSpaceIndex = line.indexOf(" ");
          if (firstSpaceIndex > 0) {
            const lineNumber = parseInt(line.substring(0, firstSpaceIndex), 10);
            if (!isNaN(lineNumber)) {
              let remainingText = trimSpacesOnly(
                line.substring(firstSpaceIndex),
              );
              let isBreakpointLine = false;

              // Process the line content and remove any prefixes
              // Remove "B->" prefix (breakpoint + current line)
              if (remainingText.startsWith("B->")) {
                remainingText = remainingText.substring(3);
                isBreakpointLine = true;

                // IMPORTANT: Only set current line from 'where' or step commands, not from 'll'
                if (cmd !== "ll") {
                  newCurrentLine = lineNumber;
                }
              }
              // Remove "->" prefix (current line)
              else if (remainingText.startsWith("->")) {
                remainingText = remainingText.substring(2);

                // IMPORTANT: Only set current line from 'where' or step commands, not from 'll'
                if (cmd !== "ll") {
                  newCurrentLine = lineNumber;
                }
              }
              // Remove "B" prefix (breakpoint)
              else if (remainingText.startsWith("B")) {
                remainingText = remainingText.substring(1);
                isBreakpointLine = true;
              }

              // Check if this line has a breakpoint in our map or is marked as a breakpoint line
              const hasBreakpoint = Boolean(
                (newCurrentFile &&
                  lineBreakpoints.has(`${newCurrentFile}:${lineNumber}`)) ||
                  isBreakpointLine,
              );

              parsedLines.push({
                lineNumber,
                content: trimSpacesOnly(remainingText).substring(1),
                isBreakpoint: hasBreakpoint,
              });

              foundSourceCode = true;
            }
          }
        }
      }

      if (newCurrentLine !== null && cmd !== "ll") {
        console.log(
          `Setting current line to ${newCurrentLine} from '${cmd}' command output`,
        );
        setCurrentLine(newCurrentLine);
      }

      if (cmd === "ll" && foundSourceCode && parsedLines.length > 0) {
        // When updating source code from 'll' command, preserve breakpoint status
        // from both our lineBreakpoints map and any existing breakpoints
        if (newCurrentFile) {
          parsedLines = parsedLines.map((line) => ({
            ...line,
            isBreakpoint:
              line.isBreakpoint ||
              lineBreakpoints.has(`${newCurrentFile}:${line.lineNumber}`),
          }));
        }

        setSourceCode(parsedLines);
      } else if (output.includes("Breakpoint") && output.includes("at")) {
        console.log("Skipping breakpoint list output for source code view");
      }
    } finally {
      setIsParsingOutput(false);
    }
  };

  // Function to fetch and record all breakpoints from PDB
  const fetchAndRecordAllBreakpoints = async () => {
    if (
      !selectedBreakpoint ||
      !selectedBreakpoint.taskId ||
      !shouldFetchBreakpoints()
    ) {
      console.log("Skipping breakpoint fetch due to state:", {
        isFetching: isFetchingBreakpoints,
        isLongPolling,
        isExecuting: executing,
      });
      return false;
    }

    isBreakpointsFetchingRef.current = true;

    try {
      setIsFetchingBreakpoints(true);

      const response = await sendDebugCommand({
        job_id: jobId,
        task_id: selectedBreakpoint.taskId,
        cmd: "b\n", // List all breakpoints in pdb
      });

      if (response) {
        // Create a new map to store breakpoint information
        const newBreakpointMap = new Map<string, number>();

        // Parse the response to extract breakpoint information
        const lines = response.split("\n");

        // Skip the header line
        for (let i = 1; i < lines.length; i++) {
          const line = lines[i].trim();
          if (!line || line === "(Pdb) ") {
            continue;
          }

          try {
            // First, extract the breakpoint number from the beginning of the line
            const firstSpaceIndex = line.indexOf(" ");
            if (firstSpaceIndex < 1) {
              continue;
            }

            const bpIndexStr = line.substring(0, firstSpaceIndex).trim();
            const bpIndex = parseInt(bpIndexStr, 10);
            if (isNaN(bpIndex)) {
              continue;
            }

            // Find 'at' in the line followed by the file path and line number
            const atIndex = line.indexOf(" at ");
            if (atIndex < 0) {
              continue;
            }

            // Extract everything after 'at ' which is the file path and line number
            const locationPart = line.substring(atIndex + 4).trim();
            const lastColonIndex = locationPart.lastIndexOf(":");

            if (lastColonIndex > 0) {
              const filename = locationPart.substring(0, lastColonIndex);
              const lineNumber = parseInt(
                locationPart.substring(lastColonIndex + 1),
                10,
              );

              if (!isNaN(lineNumber)) {
                const key = `${filename}:${lineNumber}`;
                console.log(`Storing breakpoint index ${bpIndex} for ${key}`);
                newBreakpointMap.set(key, bpIndex);

                // If we don't have a current file yet but we have breakpoints, set it
                if (!currentFile && filename) {
                  console.log(
                    "Setting current file to first breakpoint file:",
                    filename,
                  );
                  setCurrentFile(filename);
                }

                // Update source code to show this breakpoint if it's in the current file
                // Note: currentFile might be null initially, so we need to handle that
                if (
                  currentFile &&
                  (currentFile === filename ||
                    filename.endsWith(currentFile) ||
                    currentFile.endsWith(filename))
                ) {
                  console.log(
                    "Updating source code for breakpoint at line",
                    lineNumber,
                  );
                  setSourceCode((prev) =>
                    prev.map((srcLine) =>
                      srcLine.lineNumber === lineNumber
                        ? { ...srcLine, isBreakpoint: true }
                        : srcLine,
                    ),
                  );
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
    } finally {
      setIsFetchingBreakpoints(false);
      setExecuting(false);
      isBreakpointsFetchingRef.current = false;
    }

    return true; // Return success for callers to know it completed
  };

  // Execute debug command and handle the response
  const executeDebugCommand = async (cmd: string): Promise<string | null> => {
    // Check if we're already long polling
    if (isLongPolling) {
      console.log("Long polling in progress, ignoring command:", cmd);
      return null;
    }

    if (!cmd.trim() || !jobId || !currentTaskId || !selectedBreakpoint) {
      return null;
    }

    // Check if session is invalid
    if (invalidSessions.has(currentTaskId)) {
      setExecutionMessage(
        "This debug session is invalid. Please select a different breakpoint.",
      );
      setTimeout(() => setExecutionMessage(null), 3000);
      return null;
    }

    // Prevent multiple commands from running simultaneously
    if (executing) {
      console.log("Ignoring command, another command is already executing");
      return null;
    }

    // Check if this is a potentially long-running command like continue or step commands
    const isLongRunningCommand = [
      "c",
      "continue",
      "n",
      "next",
      "s",
      "step",
      "r",
      "return",
    ].includes(cmd.trim());

    let pollTimeout: NodeJS.Timeout | null = null;
    const startTime = Date.now();
    const MAX_POLL_TIME = 60000; // 60 seconds timeout

    // Clear any existing polling timeouts to prevent command overlap
    if (refreshInterval) {
      clearInterval(refreshInterval);
      setRefreshInterval(null);
    }

    try {
      setExecuting(true);
      if (isLongRunningCommand) {
        setIsLongPolling(true);
      }

      // Send the initial command
      const response = await sendDebugCommand({
        job_id: jobId,
        task_id: currentTaskId,
        cmd: cmd + "\n", // Add newline for command termination
      });

      console.log("Debug command response:", response);

      // For long-running commands, if we don't get a proper response, start polling
      if (isLongRunningCommand && (!response || !response.includes("(Pdb)"))) {
        const cmdName = ["n", "next"].includes(cmd.trim())
          ? "Step Over"
          : ["s", "step"].includes(cmd.trim())
          ? "Step Into"
          : ["r", "return"].includes(cmd.trim())
          ? "Step Out"
          : "Continue";

        setExecutionMessage(
          `${cmdName} command is running... Waiting for completion`,
        );

        // Start polling until we get a proper response or timeout
        return new Promise<string | null>((resolve) => {
          // Track if the polling has been completed to prevent race conditions
          let isPollingComplete = false;

          const pollForResponse = async () => {
            // Prevent multiple polling calls from executing simultaneously
            if (isPollingComplete) {
              console.log("Polling already completed, skipping this poll call");
              return;
            }

            try {
              // Check if we've exceeded the timeout
              if (Date.now() - startTime > MAX_POLL_TIME) {
                console.log("Command polling timeout exceeded");
                setExecutionMessage(
                  "Command timed out - Automatically closing debug session and disabling breakpoint...",
                );

                // Mark the polling as complete to prevent further polling
                isPollingComplete = true;

                // Mark the breakpoint as invalid
                if (selectedBreakpoint) {
                  // Clear all line breakpoints first if we have any
                  if (lineBreakpoints.size > 0 && selectedBreakpoint.taskId) {
                    try {
                      // Convert Map to array of [key, value] pairs and process sequentially
                      const breakpointEntries = Array.from(
                        lineBreakpoints.entries(),
                      );

                      // eslint-disable-next-line
                      for (const [key, bpIndex] of breakpointEntries) {
                        try {
                          // Send clear command with the specific breakpoint index
                          await sendDebugCommand({
                            job_id: jobId,
                            task_id: selectedBreakpoint.taskId,
                            cmd: `clear ${bpIndex}\n`,
                          });
                          console.log(`Cleared breakpoint at index ${bpIndex}`);
                        } catch (error) {
                          console.error(
                            `Failed to clear breakpoint at index ${bpIndex}:`,
                            error,
                          );
                          // Continue with the next breakpoint even if one fails
                        }
                      }
                      // Clear the breakpoints map
                      setLineBreakpoints(new Map());
                    } catch (error) {
                      console.error(
                        "Failed to clear line breakpoints on timeout:",
                        error,
                      );
                      // Continue with closing the session even if clearing breakpoints fails
                    }
                  }

                  // First close the debug session
                  try {
                    // eslint-disable-next-line
                    await closeDebugSession(jobId!, selectedBreakpoint.taskId!);
                    setExecutionMessage(
                      "Debug session closed due to timeout. The breakpoint has been disabled.",
                    );
                  } catch (error) {
                    console.error(
                      "Failed to close timed out debug session:",
                      error,
                    );
                    setExecutionMessage(
                      "Failed to close debug session cleanly. Please try closing it manually.",
                    );
                  }

                  // Update breakpoints list to mark this as invalid
                  setBreakpoints((prev) =>
                    prev.map((bp) =>
                      bp.taskId === selectedBreakpoint.taskId
                        ? { ...bp, enable: false }
                        : bp,
                    ),
                  );

                  // Add this task ID to invalid sessions
                  if (currentTaskId) {
                    setInvalidSessions((prev) =>
                      new Set(prev).add(currentTaskId),
                    );

                    // Important: Clear the current task ID and selected breakpoint
                    // to prevent users from interacting with invalid session
                    setCurrentTaskId(null);
                    setSelectedBreakpoint(null);
                  }
                }

                // Clear UI state on timeout
                setSourceCode([]);
                setCurrentLine(null);
                setLineBreakpoints(new Map());
                setOutput("");

                // After a brief delay, clear the error message and reset state
                setTimeout(() => {
                  setExecutionMessage(
                    "Session timed out. Please select a different breakpoint or set a new one.",
                  );
                  setTimeout(() => setExecutionMessage(null), 5000);
                }, 2000);

                resolve(null);
                return;
              }

              // Use a simple status check without operation tracking
              try {
                // Send an empty command to check if we're back at the prompt
                const statusResponse = await sendDebugCommand({
                  job_id: jobId,
                  task_id: currentTaskId,
                  cmd: "",
                });

                console.log("Polling response:", statusResponse);

                // If we get a response with a Pdb prompt, the command has finished
                if (statusResponse && statusResponse.includes("(Pdb)")) {
                  console.log("Command completed, back at prompt");

                  // Mark polling as complete to prevent further polling
                  isPollingComplete = true;

                  // Extract line information directly from the status response
                  // instead of making an additional 'where' call
                  try {
                    // Parse the status response directly for line information
                    parseDebugOutput(statusResponse, cmd);

                    // If the current line is updated from the status response,
                    // update the source code to highlight it
                    if (currentLine !== null) {
                      // Update the source code to highlight the current line
                      setSourceCode((prevSourceCode) =>
                        prevSourceCode.map((line) => ({
                          ...line,
                          isCurrent: line.lineNumber === currentLine,
                        })),
                      );

                      // Make sure the current line is visible
                      setTimeout(() => {
                        if (sourceCodeRef.current && currentLine) {
                          const lineElement =
                            sourceCodeRef.current.querySelector(
                              `[data-line="${currentLine}"]`,
                            );
                          if (lineElement) {
                            lineElement.scrollIntoView({
                              behavior: "smooth",
                              block: "center",
                            });
                          }
                        }
                      }, 100);
                    }
                  } catch (error) {
                    console.error(
                      "Error updating line information from command response:",
                      error,
                    );
                  }

                  // Only fetch breakpoints if we actually need to
                  setTimeout(() => {
                    if (currentFile && shouldFetchBreakpoints()) {
                      fetchAndRecordAllBreakpoints().catch((err) =>
                        console.error(
                          "Error fetching breakpoints after polling:",
                          err,
                        ),
                      );
                    }
                  }, 100);

                  setExecutionMessage(null);
                  setIsLongPolling(false);
                  resolve(statusResponse);
                } else {
                  // Continue polling if not complete and we haven't exceeded timeout
                  if (!isPollingComplete) {
                    pollTimeout = setTimeout(pollForResponse, 1000);
                  }
                }
              } catch (error) {
                console.error("Error during status check:", error);
                // Try to continue polling despite error
                if (!isPollingComplete) {
                  pollTimeout = setTimeout(pollForResponse, 1000);
                }
              }
            } catch (error) {
              console.error("Error during polling:", error);
              setIsLongPolling(false);
              isPollingComplete = true; // Mark as complete to stop polling
              resolve(null);
            }
          };

          // Start the polling
          pollTimeout = setTimeout(pollForResponse, 1000);
        }).finally(async () => {
          // Clean up the timeout if it exists
          if (pollTimeout) {
            clearTimeout(pollTimeout);
          }

          // Ensure we're not in executing or long polling state
          setExecuting(false);
          setIsLongPolling(false);

          // After long polling is done, don't automatically refresh breakpoints
          // This was causing unnecessary network requests
          // Only refresh if the UI state is out of sync (controlled by isLoading flag)
          if (selectedBreakpoint && selectedBreakpoint.taskId && isLoading) {
            try {
              await fetchBreakpoints();
            } catch (error) {
              console.error(
                "Error refreshing breakpoints after command:",
                error,
              );
            }
          }
        });
      }

      // Parse the response with knowledge of the command
      await parseDebugOutput(response, cmd);

      // After step into or step out commands, refresh source code but preserve the current line
      if (
        ["n", "next", "s", "step", "r", "return"].includes(cmd.trim()) &&
        currentLine !== null
      ) {
        console.log(
          `After ${cmd} command, refreshing source code with 'll' while preserving current line: ${currentLine}`,
        );

        // Store the current line so we can restore it after getting updated source
        const savedCurrentLine = currentLine;

        // Get fresh source code with 'll' command
        const listResponse = await sendDebugCommand({
          job_id: jobId,
          // eslint-disable-next-line
          task_id: selectedBreakpoint!.taskId!,
          cmd: "ll\n",
        });

        if (listResponse) {
          // Parse the 'll' output for source code but don't update the current line
          await parseDebugOutput(listResponse, "ll");

          // Restore the current line marker after 'll' is processed
          setTimeout(() => {
            console.log(
              `Explicitly restoring current line to ${savedCurrentLine} after 'll' command`,
            );
            setSourceCode((prevSourceCode) =>
              prevSourceCode.map((line) => ({
                ...line,
                isBreakpoint: line.isBreakpoint,
              })),
            );

            // Make sure the current line is visible
            setTimeout(() => {
              if (sourceCodeRef.current && savedCurrentLine) {
                const lineElement = sourceCodeRef.current.querySelector(
                  `[data-line="${savedCurrentLine}"]`,
                );
                if (lineElement) {
                  lineElement.scrollIntoView({
                    behavior: "smooth",
                    block: "center",
                  });
                }
              }
            }, 50);
          }, 50);
        }
      } else if (
        !isLongRunningCommand &&
        sourceCode.length === 0 &&
        shouldFetchSourceCode()
      ) {
        console.log(
          `Command ${cmd} returned immediate response but no source code exists, fetching it...`,
        );
        await fetchSourceCode(false);
      } else if (!isLongRunningCommand && currentLine !== null) {
        // We already have source code, just update the current line highlight
        setSourceCode((prevSourceCode) =>
          prevSourceCode.map((line) => ({
            ...line,
            isBreakpoint: line.isBreakpoint,
          })),
        );

        // Make sure the current line is visible
        setTimeout(() => {
          if (sourceCodeRef.current && currentLine) {
            const lineElement = sourceCodeRef.current.querySelector(
              `[data-line="${currentLine}"]`,
            );
            if (lineElement) {
              lineElement.scrollIntoView({
                behavior: "smooth",
                block: "center",
              });
            }
          }
        }, 100);
      }

      setExecutionMessage(null);
      return response;
    } catch (error) {
      console.error("Failed to send debug command:", error);
      return null;
    } finally {
      // Only set executing to false if we're not in polling mode
      if (!isLongRunningCommand || !pollTimeout) {
        setExecuting(false);
        setIsLongPolling(false);
      }
    }
  };

  // Update fetchSourceCode to have an option to skip the where command
  const fetchSourceCode = async (skipWhereCommand = false) => {
    if (!selectedBreakpoint || !selectedBreakpoint.taskId) {
      return;
    }

    // Don't check shouldFetchSourceCode() here as it may falsely reject valid requests
    // Only check for direct conflicts that would cause errors
    if (executing && !isSourceCodeFetchingRef.current) {
      console.log(
        "Can't fetch source code during execution, waiting for execution to complete",
      );
      return;
    }

    // Set the flag to prevent recursion
    isSourceCodeFetchingRef.current = true;

    try {
      // Clear source code before fetching new code
      setSourceCode([]);
      setExecuting(true);

      // Start fetching breakpoints early in parallel
      const breakpointsPromise = fetchAndRecordAllBreakpoints();

      // Save current line to maintain it while refreshing source code with 'll' command
      let savedCurrentLine = currentLine;
      let hasExecutedWhereCommand = false;

      // Only get the current execution point if not skipping where
      if (!skipWhereCommand) {
        console.log("Fetching current location with 'where' command");
        const whereResponse = await sendDebugCommand({
          job_id: jobId,
          task_id: selectedBreakpoint.taskId,
          cmd: "where\n",
        });

        if (whereResponse) {
          await parseDebugOutput(whereResponse, "where");
          hasExecutedWhereCommand = true;
          // Save the new current line that was set by the where command
          savedCurrentLine = currentLine;
          console.log("Got current line from 'where':", savedCurrentLine);
        }
      }

      // Small delay to ensure state is settled
      await new Promise((resolve) => setTimeout(resolve, 50));

      // Get source code listing - ensure we preserve the current line
      console.log("Fetching source listing with 'll' command");
      const listResponse = await sendDebugCommand({
        job_id: jobId,
        task_id: selectedBreakpoint.taskId,
        cmd: "ll\n",
      });

      if (listResponse) {
        // If we have already run 'where' command and got a current line,
        // we want to make sure the source code displays with that position
        if (hasExecutedWhereCommand && savedCurrentLine !== null) {
          // Force the current line to be maintained when parsing ll output
          await parseDebugOutput(listResponse, "ll");

          // Give a moment for the state to update
          await new Promise((resolve) => setTimeout(resolve, 50));

          // Ensure the current line marker is correctly displayed
          // by explicitly updating the source code one more time
          console.log("Explicitly setting current line:", savedCurrentLine);
          setSourceCode((prevSourceCode) =>
            prevSourceCode.map((line) => ({
              ...line,
              isBreakpoint: line.isBreakpoint,
            })),
          );

          // Make sure the current line state is also set directly
          setCurrentLine(savedCurrentLine);
        } else {
          // Standard case - parse the listing
          await parseDebugOutput(listResponse, "ll");
        }
      }

      // Wait for breakpoints to finish loading
      await breakpointsPromise;

      // If we got a saved current line, make absolutely sure it's set
      if (savedCurrentLine !== null) {
        console.log("Final update of current line:", savedCurrentLine);
        setCurrentLine(savedCurrentLine);
      }

      setExecutionMessage(null);
    } catch (error) {
      console.error("Failed to fetch source code:", error);
    } finally {
      setExecuting(false);
      isSourceCodeFetchingRef.current = false;
    }
  };

  const handleSetBreakpoint = async () => {
    // Check required fields based on mode
    if (isActorMode) {
      if (!methodName) {
        alert("Please fill in the Method Name");
        return;
      }
    } else {
      if (!functionName) {
        alert("Please fill in the Function Name");
        return;
      }
    }

    try {
      setIsLoading(true);
      const result = await setBreakpoint({
        job_id: jobId,
        actor_cls: isActorMode ? actorClass || undefined : undefined,
        actor_name: isActorMode ? actorName || undefined : undefined,
        method_name: isActorMode ? methodName || undefined : undefined,
        func_name: !isActorMode ? functionName || undefined : undefined,
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
        alert(
          "Failed to set breakpoint. Please check the console for details.",
        );
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

      // Clear all line breakpoints if we have an active debug session
      if (bp.taskId && bp.taskId !== "unknown") {
        try {
          // If we have breakpoints mapped, clear each one individually by index
          if (lineBreakpoints.size > 0) {
            // Convert Map to array of [key, value] pairs and process sequentially
            const breakpointEntries = Array.from(lineBreakpoints.entries());

            // eslint-disable-next-line
            for (const [key, bpIndex] of breakpointEntries) {
              try {
                // Send clear command with the specific breakpoint index
                await sendDebugCommand({
                  job_id: jobId,
                  task_id: bp.taskId,
                  cmd: `clear ${bpIndex}\n`,
                });
                console.log(`Cleared breakpoint at index ${bpIndex}`);
              } catch (error) {
                console.error(
                  `Failed to clear breakpoint at index ${bpIndex}:`,
                  error,
                );
                // Continue with the next breakpoint even if one fails
              }
            }

            // Clear the breakpoints map after clearing all breakpoints
            setLineBreakpoints(new Map());
          } else {
            console.log("No line breakpoints found to clear");
          }
        } catch (error) {
          console.error("Failed to clear line breakpoints:", error);
          // Continue with disabling the breakpoint even if clearing line breakpoints fails
        }
      }

      const result = await setBreakpoint({
        job_id: jobId,
        actor_id: bp.actorId || undefined,
        actor_cls: bp.actorCls || undefined,
        actor_name: bp.actorName || undefined,
        method_name: bp.methodName || undefined,
        func_name: bp.funcName || undefined,
        flag: false,
      });

      if (result) {
        // eslint-disable-next-line
        if (
          bp.taskId &&
          bp.taskId !== "unknown" &&
          selectedBreakpoint &&
          selectedBreakpoint.taskId === bp.taskId
        ) {
          // eslint-disable-next-line
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
        alert(
          "Failed to disable breakpoint. Please check the console for details.",
        );
      }
    } catch (error) {
      console.error("Failed to disable breakpoint:", error);
      alert("An error occurred while disabling the breakpoint");
    } finally {
      setIsDisablingBreakpoint(false);
      setExecutionMessage(null);
    }
  };

  const handleDeleteBreakpoint = async (bp: Breakpoint) => {
    try {
      setIsLoading(true);

      // Only call closeDebugSession if the task_id is valid and not 'unknown'
      let result = true;
      if (bp.taskId && bp.taskId !== "unknown") {
        // Clear all line breakpoints before closing the session
        if (lineBreakpoints.size > 0) {
          try {
            // Convert Map to array of [key, value] pairs and process sequentially
            const breakpointEntries = Array.from(lineBreakpoints.entries());

            // eslint-disable-next-line
            for (const [key, bpIndex] of breakpointEntries) {
              try {
                // Send clear command with the specific breakpoint index
                await sendDebugCommand({
                  job_id: jobId,
                  task_id: bp.taskId,
                  cmd: `clear ${bpIndex}\n`,
                });
                console.log(`Cleared breakpoint at index ${bpIndex}`);
              } catch (error) {
                console.error(
                  `Failed to clear breakpoint at index ${bpIndex}:`,
                  error,
                );
                // Continue with the next breakpoint even if one fails
              }
            }

            // Clear the breakpoints map after clearing all breakpoints
            setLineBreakpoints(new Map());
          } catch (error) {
            console.error("Failed to clear line breakpoints:", error);
            // Continue with closing the session even if clearing line breakpoints fails
          }
        }
        // eslint-disable-next-line
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
        alert(
          "Failed to delete breakpoint. Please check the console for details.",
        );
      }
    } catch (error) {
      console.error("Failed to delete breakpoint:", error);
      alert("An error occurred while deleting the breakpoint");
    } finally {
      setIsLoading(false);
    }
  };

  const handleSelectBreakpoint = (bp: Breakpoint) => {
    if (bp.taskId && bp.taskId !== "unknown") {
      // Don't allow selecting invalid sessions
      if (invalidSessions.has(bp.taskId)) {
        setExecutionMessage(
          "This breakpoint session is invalid. Please select a different breakpoint.",
        );
        setTimeout(() => setExecutionMessage(null), 3000);
        return;
      }

      // Save current session state if we have one
      if (selectedBreakpoint?.taskId) {
        saveSessionState(selectedBreakpoint.taskId);
      }

      // Load state for the new session if it exists
      const savedState = loadSessionState(bp.taskId);

      // Update the selected breakpoint and current task ID
      setSelectedBreakpoint(bp);
      setCurrentTaskId(bp.taskId);

      if (savedState) {
        // Restore saved state
        setSourceCode(savedState.sourceCode);
        setCurrentFile(savedState.currentFile);
        setCurrentLine(savedState.currentLine);
      } else {
        // Clear state and fetch new source code
        setSourceCode([]);
        setCurrentFile(null);
        setCurrentLine(null);
        setOutput("");

        // Fetch source code after a short delay
        setTimeout(() => {
          if (bp.taskId) {
            handleBreakpointSelectionAndFetchSource();
          }
        }, 100);
      }
    }
  };

  // Debounced command executor to prevent rapid command execution
  const executeSafeCommand = async (cmd: string): Promise<string | null> => {
    // Check if the current session is invalid
    if (currentTaskId && invalidSessions.has(currentTaskId)) {
      setExecutionMessage(
        "This debug session is invalid. Please select a different breakpoint.",
      );
      setTimeout(() => setExecutionMessage(null), 3000);
      return null;
    }

    return executeDebugCommand(cmd);
  };

  const handleStepOver = async () => {
    // First ensure we're not executing anything else
    if (executing) {
      return;
    }

    updateOutput(""); // Clear output
    const response = await executeSafeCommand("n");

    // Wait a moment for state to update, then scroll to current line
    if (response) {
      setTimeout(scrollToCurrentLine, 100);
    }
  };

  const handleStepInto = async () => {
    if (executing) {
      return;
    }

    updateOutput(""); // Clear output
    const response = await executeSafeCommand("s");

    // Wait a moment for state to update, then scroll to current line
    if (response) {
      setTimeout(scrollToCurrentLine, 100);
    }
  };

  const handleStepOut = async () => {
    if (executing) {
      return;
    }

    updateOutput(""); // Clear output
    const response = await executeSafeCommand("r");

    // Wait a moment for state to update, then scroll to current line
    if (response) {
      setTimeout(scrollToCurrentLine, 100);
    }
  };

  const handleContinue = async () => {
    if (executing) {
      return;
    }

    updateOutput(""); // Clear output
    const response = await executeSafeCommand("c");

    // Wait a moment for state to update, then scroll to current line
    if (response) {
      setTimeout(scrollToCurrentLine, 100);
    }
  };

  const handleEvaluate = async (expr: string) => {
    if (
      !expr.trim() ||
      !selectedBreakpoint ||
      !selectedBreakpoint.taskId ||
      executing
    ) {
      return;
    }

    // Check if the current session is invalid
    if (currentTaskId && invalidSessions.has(currentTaskId)) {
      setExecutionMessage(
        "This debug session is invalid. Please select a different breakpoint.",
      );
      setTimeout(() => setExecutionMessage(null), 3000);
      return;
    }

    try {
      setExecuting(true);
      setExecutionMessage(`Evaluating expression...`);

      // Add cooldown check here too
      const now = Date.now();
      if (now - lastCommandTimeRef.current < COMMAND_COOLDOWN_MS) {
        console.log("Evaluation rejected: too soon after previous command");
        setExecutionMessage(null);
        return;
      }
      lastCommandTimeRef.current = now;

      const response = await sendDebugCommand({
        job_id: jobId,
        task_id: selectedBreakpoint.taskId,
        cmd: `p ${expr}\n`,
      });

      if (response) {
        // Only show the latest evaluation result
        updateOutput(response.trim().replace(/\(Pdb\)/, ""));
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
    // Prevent recursive calls
    if (isLoading) {
      console.log("Already loading breakpoints, skipping duplicate call");
      return;
    }

    try {
      setIsLoading(true);
      const breakpointsData = await getBreakpoints(jobId);
      console.log("Fetched breakpoints:", breakpointsData);

      // Use breakpoints directly since getBreakpoints now handles the response format
      setBreakpoints(breakpointsData);

      // Find active breakpoints with task_id set and not 'unknown'
      const activeBreakpoints = breakpointsData.filter(
        (bp) => bp && bp.enable && bp.taskId && bp.taskId !== "unknown",
      );

      // Initialize session state for each active breakpoint if not already present
      activeBreakpoints.forEach((bp) => {
        if (bp.taskId && !sessionStates[bp.taskId]) {
          console.log(`Initializing session state for breakpoint ${bp.taskId}`);
          const initialState: SessionState = {
            currentLine: null,
            sourceCode: [],
            currentFile: null,
          };

          // Update session states
          setSessionStates((prev) => ({
            ...prev,
            // eslint-disable-next-line
            [bp.taskId!]: initialState,
          }));

          // Save to local storage
          try {
            const storageKey = `debug_session_${jobId}_${bp.taskId}`;
            localStorage.setItem(storageKey, JSON.stringify(initialState));
          } catch (error) {
            console.error(
              "Failed to save initial session state to local storage:",
              error,
            );
          }
        }
      });

      // If there's an already selected breakpoint and it's still active, keep using it
      const currentBreakpointStillActive =
        selectedBreakpoint &&
        selectedBreakpoint.taskId &&
        activeBreakpoints.some((bp) => bp.taskId === selectedBreakpoint.taskId);

      if (activeBreakpoints.length > 0) {
        // Set current task ID to the first active breakpoint task ID only if we don't have one already
        if (!currentTaskId) {
          const firstActive = activeBreakpoints[0];
          if (firstActive.taskId) {
            setCurrentTaskId(firstActive.taskId);
          }
        }

        // If there's no selected breakpoint yet and we don't have an active session, select the first active one
        if (
          !selectedBreakpoint &&
          !currentBreakpointStillActive &&
          !currentTaskId
        ) {
          console.log("Setting selected breakpoint to:", activeBreakpoints[0]);

          // Reset state to prevent recursive calls
          setSourceCode([]);
          setCurrentFile(null);
          setCurrentLine(null);

          // Set the new breakpoint
          setSelectedBreakpoint(activeBreakpoints[0]);

          // Explicitly fetch source code after a short delay to ensure state updates have settled
          setTimeout(() => {
            if (shouldFetchSourceCode()) {
              console.log(
                "Explicitly fetching source code after auto-selecting breakpoint",
              );
              fetchSourceCode().catch((err) =>
                console.error(
                  "Error fetching source code after auto-selecting breakpoint:",
                  err,
                ),
              );
            }
          }, 150);
        } else if (currentBreakpointStillActive && sourceCode.length === 0) {
          // If we have a selected breakpoint but no source code loaded yet, fetch the source code
          console.log(
            "Selected breakpoint exists but no source code, fetching source code",
          );
          setTimeout(() => {
            if (shouldFetchSourceCode()) {
              console.log(
                "Using handleBreakpointSelectionAndFetchSource for reliable source code fetch",
              );
              handleBreakpointSelectionAndFetchSource();
            }
          }, 150);
        }
      } else {
        // No active breakpoints, clear current task ID only if it's not already set
        if (!currentTaskId) {
          setCurrentTaskId(null);
        }
        // If there was a selected breakpoint but it's no longer active, clear it
        if (selectedBreakpoint && !currentBreakpointStillActive) {
          setSelectedBreakpoint(null);
        }
      }

      // After fetching breakpoints, check if we need to load initial breakpoints
      handleInitialLoad();
    } catch (error) {
      console.error("Failed to fetch breakpoints:", error);
      // If there's an error, set an empty array to avoid "map" errors
      setBreakpoints([]);
    } finally {
      setIsLoading(false);
    }
  };

  // Toggle a breakpoint at a specific line
  const toggleLineBreakpoint = async (lineNumber: number) => {
    if (!currentFile || !selectedBreakpoint || !selectedBreakpoint.taskId) {
      return;
    }

    // Check if the current session is invalid
    if (invalidSessions.has(selectedBreakpoint.taskId)) {
      setExecutionMessage(
        "This debug session is invalid. Cannot modify breakpoints.",
      );
      setTimeout(() => setExecutionMessage(null), 3000);
      return;
    }

    // Find the line in source code
    const lineData = sourceCode.find((line) => line.lineNumber === lineNumber);
    if (!lineData) {
      return;
    }

    try {
      setExecuting(true);

      if (lineData.isBreakpoint) {
        // Clear the breakpoint
        const key = `${currentFile}:${lineNumber}`;
        const bpIndex = lineBreakpoints.get(key);

        if (bpIndex !== undefined) {
          setExecutionMessage(`Clearing breakpoint at line ${lineNumber}...`);

          // Use the Pdb 'clear' command with the stored breakpoint index
          const response = await sendDebugCommand({
            job_id: jobId,
            task_id: selectedBreakpoint.taskId,
            cmd: `clear ${bpIndex}\n`,
          });

          console.log("Clear breakpoint response:", response);

          // Immediately update the UI
          setSourceCode((prev) =>
            prev.map((line) =>
              line.lineNumber === lineNumber
                ? { ...line, isBreakpoint: false }
                : line,
            ),
          );

          // Remove from map
          setLineBreakpoints((prev) => {
            const newMap = new Map(prev);
            newMap.delete(key);
            return newMap;
          });
        } else {
          console.error("Could not find breakpoint index for line", lineNumber);
          setTimeout(() => setExecutionMessage(null), 2000);
        }
      } else {
        // Use the Pdb 'b' command to set a breakpoint
        const response = await sendDebugCommand({
          job_id: jobId,
          task_id: selectedBreakpoint.taskId,
          cmd: `b ${lineNumber}\n`,
        });

        console.log("Set breakpoint response:", response);
      }

      // Always update breakpoints after toggling
      await fetchAndRecordAllBreakpoints();

      setExecutionMessage(null);
    } catch (error) {
      console.error("Error toggling breakpoint:", error);
      setTimeout(() => setExecutionMessage(null), 3000);
    } finally {
      setExecuting(false);
    }
  };

  // Add handler for debug mode toggle
  const handleDebugModeToggle = async () => {
    if (!jobId) {
      return;
    }

    try {
      setIsLoading(true);
      const success = await switchDebugMode(jobId, !debugMode);
      if (success) {
        setDebugMode(!debugMode);
      }
    } catch (error) {
      console.error("Failed to switch debug mode:", error);
    } finally {
      setIsLoading(false);
    }
  };

  if (!open) {
    return null;
  }

  const renderContent = () => {
    return (
      <React.Fragment>
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

        <div className="debug-mode-switch">
          <label className="switch-label">
            <span>Debug Mode</span>
            <input
              type="checkbox"
              checked={debugMode}
              onChange={handleDebugModeToggle}
              disabled={isLoading || !jobId}
            />
            <span className="switch-slider"></span>
          </label>
        </div>

        {/* Display warning for invalid sessions */}
        {selectedBreakpoint &&
          currentTaskId &&
          invalidSessions.has(currentTaskId) && (
            <div className="invalid-session">
              This debug session has timed out and is now invalid. Please select
              a different breakpoint.
            </div>
          )}

        <div className="debug-panel-section">
          <div className="section-header">
            <h4>
              <button
                className="toggle-button"
                onClick={() => setBreakpointSectionOpen(!breakpointSectionOpen)}
                aria-label={
                  breakpointSectionOpen ? "Collapse section" : "Expand section"
                }
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
                      checked={isActorMode}
                      onChange={() => {
                        setIsActorMode(true);
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
                      checked={!isActorMode}
                      onChange={() => {
                        setIsActorMode(false);
                        setActorClass("");
                        setActorName("");
                        setMethodName("");
                      }}
                      disabled={isLoading}
                    />
                    <label htmlFor="function-type">Function</label>
                  </div>
                </div>
              </div>

              {isActorMode ? (
                <div className="actor-inputs">
                  <div className="form-row compact">
                    <input
                      type="text"
                      placeholder="Actor Class (optional)"
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
                aria-label={
                  breakpointListOpen ? "Collapse section" : "Expand section"
                }
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
                  {breakpoints.map((bp, index) => {
                    // Determine if this breakpoint is selectable (only active breakpoints are selectable)
                    const isActive = bp.taskId && bp.taskId !== "unknown";
                    // Determine if this breakpoint is currently selected
                    const isSelected =
                      selectedBreakpoint &&
                      selectedBreakpoint.taskId === bp.taskId;
                    // Determine if any breakpoint is selected (we can only select one at a time)
                    const hasSelectedBreakpoint =
                      selectedBreakpoint &&
                      selectedBreakpoint.taskId &&
                      selectedBreakpoint.taskId !== bp.taskId;
                    // Determine if this breakpoint is disabled in the UI (because another one is selected)
                    const isDisabledInUI =
                      isActive && hasSelectedBreakpoint && !isSelected;

                    return (
                      <li
                        key={index}
                        className={`breakpoint-item 
                                   ${isActive ? "active-breakpoint" : ""} 
                                   ${isSelected ? "selected-breakpoint" : ""} 
                                   ${
                                     isDisabledInUI ? "disabled-breakpoint" : ""
                                   }`}
                        title={
                          isDisabledInUI
                            ? "Close the current debug session before selecting this breakpoint"
                            : isActive
                            ? "Click to select this active breakpoint for debugging"
                            : ""
                        }
                        onClick={() => {
                          // Only handle click if breakpoint is active and enabled
                          if (isActive && bp.enable && !isDisabledInUI) {
                            handleSelectBreakpoint(bp);
                            if (setActiveDebugSession) {
                              setActiveDebugSession(bp);
                            }
                          }
                        }}
                        style={{
                          cursor:
                            isActive && bp.enable && !isDisabledInUI
                              ? "pointer"
                              : "default",
                        }}
                      >
                        <span>
                          {bp.methodName
                            ? // If methodName exists, it's an actor method breakpoint
                              `${bp.actorCls || ""}${
                                bp.actorCls && bp.actorName ? "." : ""
                              }${bp.actorName || ""}${
                                bp.actorCls || bp.actorName ? "." : ""
                              }${bp.methodName}`
                            : // If no methodName but funcName exists, it's a function breakpoint
                              `Function: ${bp.funcName || ""}`}
                          {bp.enable ? " (Enabled)" : " (Disabled)"}
                          {isActive ? " (Active ⚡)" : ""}
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
                            title={
                              isActive
                                ? "Close debug session and disable breakpoint"
                                : "Disable breakpoint"
                            }
                          >
                            {isActive ? "⏹️" : "⏸️"}
                          </button>
                        </div>
                      </li>
                    );
                  })}
                </ul>
              )}
            </div>
          )}
        </div>

        {/* Debug controls */}
        {selectedBreakpoint && selectedBreakpoint.taskId && (
          <React.Fragment>
            {/* Debug Control Buttons - replaced with icons */}
            <div className="debug-controls">
              <button
                className="debug-icon-only"
                onClick={handleStepOver}
                disabled={executing}
                title="Step Over (n)"
              >
                ⤵️
              </button>
              <button
                className="debug-icon-only"
                onClick={handleStepInto}
                disabled={executing}
                title="Step Into (s)"
              >
                ⬇️
              </button>
              <button
                className="debug-icon-only"
                onClick={handleStepOut}
                disabled={executing}
                title="Step Out (r)"
              >
                ⬆️
              </button>
              <button
                className="debug-icon-only"
                onClick={handleContinue}
                disabled={executing}
                title="Continue (c)"
              >
                ▶️
              </button>
              <button
                className="debug-icon-only close-button"
                onClick={() =>
                  selectedBreakpoint &&
                  handleDeleteBreakpoint(selectedBreakpoint)
                }
                disabled={executing || !selectedBreakpoint}
                title="Close Debug Session"
              >
                ❌
              </button>
            </div>

            {/* Execution status */}
            {executionMessage && (
              <div className="execution-status">
                <div className="status-message">
                  {executing && <span className="spinner"></span>}
                  {executionMessage}
                </div>
              </div>
            )}

            {/* Source code view */}
            <div className="debug-panel-section">
              <div className="section-header">
                <h4>
                  <button
                    className="toggle-button"
                    onClick={() =>
                      setSourceCodeSectionOpen(!sourceCodeSectionOpen)
                    }
                    aria-label={
                      sourceCodeSectionOpen
                        ? "Collapse section"
                        : "Expand section"
                    }
                  >
                    {sourceCodeSectionOpen ? "▼" : "►"}
                  </button>
                  Source Code
                </h4>
                <div className="section-actions">
                  <span className="source-code-help">
                    Click on line numbers to set/clear breakpoints
                  </span>
                </div>
              </div>
              <select
                className="session-dropdown"
                value={currentTaskId || ""}
                onChange={(e) => {
                  const taskId = e.target.value;
                  if (taskId) {
                    const selectedBp = breakpoints.find(
                      (bp) => bp.taskId === taskId,
                    );
                    if (selectedBp) {
                      handleSelectBreakpoint(selectedBp);
                      if (setActiveDebugSession) {
                        setActiveDebugSession(selectedBp);
                      }
                    }
                  }
                }}
                disabled={executing || isLoading}
              >
                <option value="" disabled>
                  Select a debug session
                </option>
                {breakpoints
                  .filter(
                    (bp) =>
                      bp.taskId &&
                      bp.taskId !== "unknown" &&
                      bp.enable &&
                      !invalidSessions.has(bp.taskId),
                  )
                  .map((bp, index) => (
                    <option key={index} value={bp.taskId}>
                      {bp.methodName
                        ? `${bp.actorCls || ""}${
                            bp.actorCls && bp.actorName ? "." : ""
                          }${bp.actorName || ""}${
                            bp.actorCls || bp.actorName ? "." : ""
                          }${bp.methodName}`
                        : `Function: ${bp.funcName || ""}`}
                    </option>
                  ))}
              </select>

              {sourceCodeSectionOpen && (
                <div className="source-code-view" ref={sourceCodeRef}>
                  {sourceCode.length === 0 ? (
                    <div className="empty-message">
                      No source code available
                    </div>
                  ) : (
                    <div className="code-display-container">
                      {/* Create a complete code string with line numbers and current line/breakpoint markers */}
                      <div className="line-markers">
                        {sourceCode.map((line, index) => {
                          const isCurrentLine = line.lineNumber === currentLine;
                          const hasBreakpoint = line.isBreakpoint;

                          return (
                            <div
                              key={line.lineNumber}
                              className={`line-marker ${
                                isCurrentLine ? "current-line-marker" : ""
                              }`}
                              data-line={line.lineNumber}
                              onClick={() => {
                                if (
                                  selectedBreakpoint &&
                                  selectedBreakpoint.taskId
                                ) {
                                  toggleLineBreakpoint(line.lineNumber);
                                }
                              }}
                              title={
                                hasBreakpoint
                                  ? "Click to remove breakpoint"
                                  : "Click to set breakpoint"
                              }
                            >
                              <span className="marker-content">
                                {hasBreakpoint && (
                                  <span className="breakpoint-indicator">
                                    ●
                                  </span>
                                )}
                                {isCurrentLine && (
                                  <span className="current-line-arrow">→</span>
                                )}
                                {line.lineNumber}
                              </span>
                            </div>
                          );
                        })}
                      </div>

                      <div className="code-content" ref={codeContentRef}>
                        <SyntaxHighlighter
                          language="python"
                          style={vscDarkPlus}
                          customStyle={{
                            margin: 0,
                            padding: "0.5em 0.5em 0.5em 1em",
                            background: "transparent",
                            fontSize: "13px",
                            lineHeight: "1.5",
                            tabSize: 4,
                            MozTabSize: "4",
                            fontFamily:
                              "'SFMono-Regular', Consolas, 'Liberation Mono', Menlo, monospace",
                            whiteSpace: "pre",
                          }}
                          showLineNumbers={false}
                          wrapLines={true}
                          useInlineStyles={true}
                          PreTag={({ children, ...props }) => (
                            <pre
                              {...props}
                              style={{
                                margin: 0,
                                whiteSpace: "pre",
                                tabSize: 4,
                                MozTabSize: "4",
                              }}
                            >
                              {children}
                            </pre>
                          )}
                          CodeTag={({ children, ...props }) => (
                            <code
                              {...props}
                              style={{
                                whiteSpace: "pre",
                                tabSize: 4,
                                MozTabSize: "4",
                              }}
                            >
                              {children}
                            </code>
                          )}
                          lineProps={(lineNumber) => {
                            const sourceLine = sourceCode[lineNumber - 1];
                            if (!sourceLine) {
                              return {};
                            }

                            const isCurrentLine =
                              sourceLine.lineNumber === currentLine;
                            const hasBreakpoint = sourceLine.isBreakpoint;

                            return {
                              style: {
                                display: "block",
                                backgroundColor: isCurrentLine
                                  ? "#2d4151"
                                  : hasBreakpoint
                                  ? "rgba(255, 0, 0, 0.2)"
                                  : "transparent",
                                borderLeft:
                                  isCurrentLine && hasBreakpoint
                                    ? "3px solid #ff5555"
                                    : "none",
                                height: "19.5px",
                                padding: "0 0.5em",
                                whiteSpace: "pre",
                                tabSize: 4,
                                MozTabSize: "4",
                              },
                              "data-line-number": sourceLine.lineNumber,
                            };
                          }}
                        >
                          {sourceCode.map((line) => line.content).join("\n")}
                        </SyntaxHighlighter>
                      </div>
                    </div>
                  )}
                </div>
              )}
            </div>

            {/* Expression Evaluation - MOVED UNDER SOURCE CODE */}
            <div className="evaluate-section">
              <div className="evaluate-input">
                <input
                  type="text"
                  placeholder="Enter expression to evaluate..."
                  onKeyDown={(e) => {
                    if (e.key === "Enter" && e.currentTarget.value) {
                      handleEvaluate(e.currentTarget.value);
                      e.currentTarget.value = "";
                    }
                  }}
                  disabled={executing}
                />
                <button
                  className="evaluate-button"
                  onClick={() => {
                    const input = document.querySelector(
                      ".evaluate-input input",
                    ) as HTMLInputElement;
                    if (input && input.value) {
                      handleEvaluate(input.value);
                      input.value = "";
                    }
                  }}
                  disabled={executing}
                >
                  Evaluate
                </button>
              </div>
            </div>

            {/* Debug Output Panel (replaces terminal) */}
            <div className="debug-panel-section">
              <div className="section-header">
                <h4>
                  <button
                    className="toggle-button"
                    onClick={() => setOutputSectionOpen(!outputSectionOpen)}
                    aria-label={
                      outputSectionOpen ? "Collapse section" : "Expand section"
                    }
                  >
                    {outputSectionOpen ? "▼" : "►"}
                  </button>
                  Eval Results
                </h4>
                <button
                  className="clear-button"
                  onClick={() => updateOutput("")}
                  disabled={executing}
                  title="Clear Results"
                >
                  Clear
                </button>
              </div>

              {outputSectionOpen && (
                <div className="debug-output-view" ref={outputRef}>
                  <pre className="output-container">{output}</pre>
                </div>
              )}
            </div>
          </React.Fragment>
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
      </React.Fragment>
    );
  };

  return (
    <div className={isTab ? "debug-panel-as-tab" : "debug-panel"}>
      {isLongPolling && (
        <div className="long-polling-message">
          Long-running command in progress. Please wait...
        </div>
      )}
      <div className={isLongPolling ? "long-polling" : ""}>
        {renderContent()}
      </div>
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
          scrollbar-width: none; /* Firefox */
          -ms-overflow-style: none; /* IE and Edge */
        }

        .source-code-view::-webkit-scrollbar {
          display: none; /* Chrome, Safari and Opera */
        }
        
        .code-display-container {
          display: flex;
          width: 100%;
          position: relative;
          font-family: 'SFMono-Regular', Consolas, 'Liberation Mono', Menlo, monospace;
        }
        
        .line-markers {
          position: sticky;
          left: 0;
          top: 0;
          bottom: 0;
          width: 45px;
          z-index: 10;
          background-color: rgba(30, 30, 30, 0.8);
          border-right: 1px solid #333;
          user-select: none;
          padding-top: 0.5em;
          overflow: hidden;
        }
        
        .line-marker {
          padding: 0 5px 10px 12px;
          text-align: right;
          line-height: 19.5px;
          cursor: pointer;
          color: #858585;
          position: relative;
          height: 19.5px;
          display: flex;
          align-items: center;
          justify-content: flex-end;
          font-size: 12px;
        }
        
        .line-marker:hover {
          color: #ffffff;
          background-color: rgba(255, 255, 255, 0.05);
        }
        
        .current-line-marker {
          color: #ffffff;
        }
        
        .marker-content {
          position: relative;
          display: inline-flex;
          align-items: center;
          height: 19.5px;
        }
        
        .current-line-arrow {
          transform: translateY(-10%);
          font-size: 14px;
        }
        
        .code-content {
          flex: 1;
          overflow-x: auto;
          font-family: 'SFMono-Regular', Consolas, 'Liberation Mono', Menlo, monospace;
          scrollbar-width: none;
          -ms-overflow-style: none;
        }

        .code-content::-webkit-scrollbar {
          display: none;
        }

        /* Override any syntax highlighter specific styles */
        .code-content span {
          font-family: 'SFMono-Regular', Consolas, 'Liberation Mono', Menlo, monospace !important;
          line-height: 19.5px !important;
        }
        
        .debug-output-view {
          max-height: 200px;
          overflow: auto;
          background-color: #1e1e1e;
          border-radius: 4px;
          color: #f0f0f0;
          margin-top: 5px;
          padding: 8px;
          scrollbar-width: none;
          -ms-overflow-style: none;
        }

        .debug-output-view::-webkit-scrollbar {
          display: none;
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
          position: relative;
        }
        
        .current-line {
          background-color: #2d4151;
        }
        
        .breakpoint-line {
          background-color: rgba(255, 0, 0, 0.2);
        }

        .breakpoint-line.current-line {
          background-color: #2d4151;
          border-left: 3px solid #ff5555;
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
          left: -12px;
          top: -1px;
        }
        
        .current-line-arrow {
          color: #00ffff;
          position: absolute;
          left: -15px;
          top: 1px;
          font-weight: bold;
        }
        
        .line-content {
          flex: 1;
          tab-size: 4;
          -moz-tab-size: 4;
          -o-tab-size: 4;
          font-family: 'SFMono-Regular', Consolas, 'Liberation Mono', Menlo, monospace;
          font-variant-ligatures: none;
          unicode-bidi: embed;
          display: flex;
          align-items: center;
        }
        
        /* Override syntax highlighter default styles */
        .line-content code {
          background: transparent !important;
          padding: 0 !important;
          margin: 0 !important;
          white-space: pre !important;
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
          gap: 16px;
          margin: 12px 0;
          flex-wrap: wrap;
          justify-content: center;
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
        
        .debug-icon-button {
          width: 36px;
          height: 36px;
          display: flex;
          align-items: center;
          justify-content: center;
          background-color: #f0f0f0;
          border: 1px solid #ddd;
          border-radius: 4px;
          cursor: pointer;
          font-size: 18px;
          padding: 0;
        }
        
        .debug-icon-button:hover {
          background-color: #e0e0e0;
        }
        
        .debug-icon-button:disabled {
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
        
        .status-message {
          display: flex;
          align-items: center;
          gap: 8px;
        }
        
        .spinner {
          display: inline-block;
          width: 16px;
          height: 16px;
          border: 2px solid rgba(0, 0, 0, 0.1);
          border-left-color: #1890ff;
          border-radius: 50%;
          animation: spin 1s linear infinite;
        }
        
        @keyframes spin {
          to { transform: rotate(360deg); }
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

        .invalid-session {
          background-color: #ffdddd;
          border-left: 4px solid #ff4d4f;
          padding: 8px;
          margin: 8px 0;
          color: #cf1322;
          font-weight: bold;
        }

        .long-polling {
          pointer-events: none;
          opacity: 0.7;
          cursor: not-allowed;
        }

        .long-polling-message {
          background-color: #fff3e0;
          border-left: 4px solid #ff9800;
          padding: 8px;
          margin: 8px 0;
          color: #e65100;
          font-weight: bold;
        }

        .active-session {
          display: flex;
          justify-content: space-between;
          align-items: center;
          background-color: #f5f5f5;
          padding: 8px;
          border-radius: 4px;
          margin-bottom: 8px;
        }
        
        .close-session-button {
          background-color: #f44336;
          color: white;
          border: none;
          padding: 6px 12px;
          border-radius: 4px;
          cursor: pointer;
          font-weight: bold;
        }
        
        .close-session-button:hover {
          background-color: #d32f2f;
        }
        
        .close-session-button:disabled {
          background-color: #ffcdd2;
          cursor: not-allowed;
        }
        
        .session-info-message {
          background-color: #e8f4fd;
          border-left: 4px solid #2196f3;
          padding: 8px;
          margin-bottom: 12px;
          font-size: 13px;
        }
        
        .session-info-message p {
          margin: 0;
          line-height: 1.4;
        }

        .disabled-breakpoint {
          opacity: 0.6;
          cursor: not-allowed;
          position: relative;
        }
        
        .disabled-breakpoint::after {
          content: '';
          position: absolute;
          top: 0;
          left: 0;
          right: 0;
          bottom: 0;
          background-color: rgba(0, 0, 0, 0.05);
          pointer-events: none;
        }

        .session-dropdown {
          padding: 4px 8px;
          border: 1px solid #ddd;
          border-radius: 4px;
          background-color: #fff;
          margin: 8px 0;
          width: 100%;
          font-size: 13px;
          height: 28px;
        }
        
        .active-session-info {
          display: flex;
          flex-direction: column;
          gap: 8px;
          width: 100%;
        }
        
        .session-item {
          display: flex;
          align-items: center;
        }
        
        .session-value {
          margin-left: 8px;
          font-family: monospace;
          word-break: break-all;
        }
      `}</style>
    </div>
  );
};

export default DebugPanel;
