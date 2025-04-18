import AnalyticsIcon from "@mui/icons-material/Analytics";
import {
  Alert,
  Box,
  Button,
  CircularProgress,
  Collapse,
  Paper,
  TextField,
  Typography,
} from "@mui/material";
import ReactMarkdown from "react-markdown";
import remarkGfm from "remark-gfm";
import { Prism as SyntaxHighlighter } from "react-syntax-highlighter";
import { materialLight } from "react-syntax-highlighter/dist/esm/styles/prism";
import React, { useEffect, useState } from "react";
import { 
  getInsightAnalyzePrompt, 
  generateReport, 
  convertTimestampsInObject,
  replaceIdsWithNames
} from "../service/insight";
import RefreshIcon from "@mui/icons-material/Refresh";
import SettingsIcon from "@mui/icons-material/Settings";

type InsightPanelProps = {
  jobId?: string;
  graphData: any;
  physicalViewData: any;
  flameData: any;
}

// Custom markdown components for better rendering
const MarkdownComponents = {
  code({ node, inline, className, children, ...props }: any) {
    const match = /language-(\w+)/.exec(className || "");
    const language = match && match[1] ? match[1] : "";
    
    return !inline && language ? (
      <SyntaxHighlighter
        style={materialLight}
        language={language}
        PreTag="div"
        {...props}
      >
        {String(children).replace(/\n$/, "")}
      </SyntaxHighlighter>
    ) : (
      <code className={className} {...props}>
        {children}
      </code>
    );
  },
  table({ children, ...props }: any) {
    return (
      <Box sx={{ overflowX: "auto", my: 2 }}>
        <table style={{ borderCollapse: "collapse", width: "100%" }} {...props}>
          {children}
        </table>
      </Box>
    );
  },
  th({ children, ...props }: any) {
    return (
      <th style={{ textAlign: "left", padding: "8px", borderBottom: "1px solid #ddd" }} {...props}>
        {children}
      </th>
    );
  },
  td({ children, ...props }: any) {
    return (
      <td style={{ padding: "8px", borderBottom: "1px solid #ddd" }} {...props}>
        {children}
      </td>
    );
  },
  a({ children, ...props }: any) {
    return (
      <a style={{ color: "#1976d2", textDecoration: "none" }} {...props}>
        {children}
      </a>
    );
  },
  img({ src, alt, ...props }: any) {
    return (
      <Box sx={{ textAlign: "center", my: 2 }}>
        <img 
          src={src} 
          alt={alt} 
          style={{ maxWidth: "100%" }}
          {...props} 
        />
      </Box>
    );
  }
};

const InsightPanel: React.FC<InsightPanelProps> = ({
  jobId,
  graphData,
  physicalViewData,
  flameData,
}) => {
  const [expanded, setExpanded] = useState<boolean>(false);
  const [loading, setLoading] = useState<boolean>(false);
  const [error, setError] = useState<string | null>(null);
  const [showSettings, setShowSettings] = useState<boolean>(false);
  const [report, setReport] = useState<string | null>(null);
  
  // Settings state
  const [openaiBaseUrl, setOpenaiBaseUrl] = useState<string>(
    localStorage.getItem("openaiBaseUrl") || ""
  );
  const [openaiApiKey, setOpenaiApiKey] = useState<string>(
    localStorage.getItem("openaiApiKey") || ""
  );
  const [openaiModel, setOpenaiModel] = useState<string>(
    localStorage.getItem("openaiModel") || ""
  );

  // Save settings to localStorage whenever they change
  useEffect(() => {
    localStorage.setItem("openaiBaseUrl", openaiBaseUrl);
    localStorage.setItem("openaiApiKey", openaiApiKey);
    localStorage.setItem("openaiModel", openaiModel);
  }, [openaiBaseUrl, openaiApiKey, openaiModel]);

  const toggleExpanded = () => {
    setExpanded(!expanded);
  };

  const generateInsightReport = async () => {
    if (!jobId) {
      setError("No job ID provided");
      return;
    }

    if (!openaiApiKey) {
      setError("OpenAI API key is required");
      return;
    }

    setLoading(true);
    setError(null);

    try {
      // 1. Get the analyze prompt
      const prompt = await getInsightAnalyzePrompt(jobId);

      // 2. Convert graph data to string with readable timestamps and replace IDs with names
      const graphDataWithReadableTime = convertTimestampsInObject(graphData);
      const graphDataWithNames = replaceIdsWithNames(graphDataWithReadableTime);
      const graphDataStr = JSON.stringify(graphDataWithNames, null, 2);

      // 3. Generate initial report
      const initialPrompt = `${prompt}\n\n{ data: ${graphDataStr} }`;
      const initialReport = await generateReport(
        initialPrompt,
        openaiApiKey,
        openaiBaseUrl,
        openaiModel
      );

      // 4. If we have physical view data, enhance the report
      let enhancedReport = initialReport;
      if (physicalViewData) {
        const physicalDataWithReadableTime = convertTimestampsInObject(physicalViewData);
        const physicalDataStr = JSON.stringify(physicalDataWithReadableTime, null, 2);
        const enhancedPrompt = `
          Given the following initial analysis:
          ${initialReport}
          
          And additional physical view data:
          ${physicalDataStr}
          
          Please enhance your analysis
          Return the enhanced analysis in the same markdown format.
          just give result markdown without any other text, do not wrap it
        `;
        
        enhancedReport = await generateReport(
          enhancedPrompt,
          openaiApiKey,
          openaiBaseUrl,
          openaiModel
        );
      }

      // 5. If we have flame graph data, further enhance the report
      let finalReport = enhancedReport;
      if (flameData) {
        const flameDataWithReadableTime = convertTimestampsInObject(flameData);
        const flameDataWithNames = replaceIdsWithNames(flameDataWithReadableTime);
        const flameDataStr = JSON.stringify(flameDataWithNames, null, 2);
        const finalPrompt = `
          Given the following analysis:
          ${enhancedReport}
          
          And additional flame graph data:
          ${flameDataStr}
          
          Please enhance your analysis
          Return the enhanced analysis in the same markdown format.
          just give result markdown without any other text, do not wrap it
        `;
        
        finalReport = await generateReport(
          finalPrompt,
          openaiApiKey,
          openaiBaseUrl,
          openaiModel
        );
      }
      const markdown_marker = "```markdown";
      const markdown_marker_end = "```";
      if (finalReport.startsWith(markdown_marker)) {
        finalReport = finalReport.slice(markdown_marker.length);
      }
      if (finalReport.endsWith(markdown_marker_end)) {
        finalReport = finalReport.slice(0, -markdown_marker_end.length);
      }
      setReport(finalReport);
      setShowSettings(false);
    } catch (error) {
      console.error("Error generating report:", error);
      setError(error instanceof Error ? error.message : "Unknown error occurred");
    } finally {
      setLoading(false);
    }
  };

  const renderReport = () => {
    if (!report) {
      return (
        <Box
          sx={{
            display: "flex",
            flexDirection: "column",
            alignItems: "center",
            justifyContent: "center",
            minHeight: "300px",
            gap: 2,
          }}
        >
          <Button
            variant="contained"
            color="primary"
            onClick={generateInsightReport}
            disabled={loading}
            startIcon={loading && <CircularProgress size={20} />}
          >
            {loading ? "Generating..." : "Generate Report"}
          </Button>
          <Button
            variant="text"
            onClick={() => setShowSettings(true)}
          >
            Settings
          </Button>
        </Box>
      );
    }

    return (
      <Box sx={{ p: 2, overflow: "auto", maxHeight: "calc(100vh - 200px)" }}>
        <Box sx={{ display: "flex", justifyContent: "flex-end", mb: 2, gap: 1 }}>
          <Button
            variant="outlined"
            onClick={generateInsightReport}
            size="small"
            disabled={loading}
            startIcon={loading ? <CircularProgress size={16} /> : <RefreshIcon />}
          >
            {loading ? "Regenerating..." : "Regenerate"}
          </Button>
          <Button
            variant="text"
            onClick={() => setShowSettings(true)}
            size="small"
            startIcon={<SettingsIcon />}
          >
            Settings
          </Button>
        </Box>
        <Box sx={{ 
          "& h1, & h2, & h3, & h4, & h5, & h6": { 
            mt: 2, 
            mb: 1,
            fontWeight: "medium" 
          },
          "& p": { 
            my: 1,
            lineHeight: 1.6
          },
          "& ul, & ol": { 
            paddingLeft: 3 
          },
          "& li": { 
            mb: 0.5 
          },
          "& blockquote": {
            borderLeft: "4px solid #e0e0e0",
            my: 1,
            pl: 2,
            py: 0.5,
            backgroundColor: "#f5f5f5"
          },
          "& pre": {
            mt: 1,
            mb: 2
          }
        }}>
          <ReactMarkdown
            remarkPlugins={[remarkGfm]}
            components={MarkdownComponents}
          >
            {report}
          </ReactMarkdown>
        </Box>
      </Box>
    );
  };

  const renderSettings = () => {
    return (
      <Box sx={{ p: 2 }}>
        <Typography variant="h6" gutterBottom>
          OpenAI API Settings
        </Typography>
        <Box sx={{ display: "flex", flexDirection: "column", gap: 2, maxWidth: "600px" }}>
          <TextField
            label="OpenAI API Base URL"
            value={openaiBaseUrl}
            onChange={(e) => setOpenaiBaseUrl(e.target.value)}
            fullWidth
            margin="normal"
            helperText=""
          />
          <TextField
            label="OpenAI API Key"
            value={openaiApiKey}
            onChange={(e) => setOpenaiApiKey(e.target.value)}
            fullWidth
            margin="normal"
            type="password"
          />
          <TextField
            label="OpenAI Model"
            value={openaiModel}
            onChange={(e) => setOpenaiModel(e.target.value)}
            fullWidth
            margin="normal"
            helperText=""
          />
          <Box sx={{ display: "flex", gap: 2, mt: 2 }}>
            <Button
              variant="contained"
              color="primary"
              onClick={() => {
                localStorage.setItem("openaiBaseUrl", openaiBaseUrl);
                localStorage.setItem("openaiApiKey", openaiApiKey);
                localStorage.setItem("openaiModel", openaiModel);
                setShowSettings(false);
              }}
            >
              Save & Close
            </Button>
            <Button
              variant="text"
              onClick={() => setShowSettings(false)}
            >
              Cancel
            </Button>
          </Box>
        </Box>
      </Box>
    );
  };

  return (
    <Box
      sx={{
        position: "absolute",
        top: 10,
        left: 110,
        zIndex: 99999,
      }}
    >
      <Button
        variant="contained"
        color="secondary"
        size="small"
        startIcon={<AnalyticsIcon />}
        onClick={toggleExpanded}
        sx={{ mb: 1, borderRadius: 2 }}
      >
        {expanded ? "Hide" : "Insight"}
      </Button>

      <Collapse in={expanded}>
        <Box
          sx={{
            width: "95vw",
            maxWidth: 1200,
            p: 2,
            background: "transparent",
          }}
        >
          <Paper
            elevation={2}
            sx={{
              borderRadius: 2,
              display: "flex",
              flexDirection: "column",
              overflow: "hidden",
              minHeight: "600px",
            }}
          >
            {error && (
              <Alert severity="error" sx={{ m: 2 }}>
                {error}
              </Alert>
            )}

            <Box sx={{ p: 0, flex: 1 }}>
              {showSettings ? renderSettings() : renderReport()}
            </Box>
          </Paper>
        </Box>
      </Collapse>
    </Box>
  );
};

export default InsightPanel; 