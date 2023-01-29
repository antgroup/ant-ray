package io.ray.streaming.jobgraph;

import com.google.common.base.Preconditions;
import io.ray.fury.types.TypeInference;
import io.ray.streaming.api.context.IndependentOperatorDescriptor;
import io.ray.streaming.api.stream.DataStream;
import io.ray.streaming.api.stream.JoinStream;
import io.ray.streaming.api.stream.Stream;
import io.ray.streaming.api.stream.StreamSink;
import io.ray.streaming.api.stream.StreamSource;
import io.ray.streaming.api.stream.UnionStream;
import io.ray.streaming.operator.AbstractStreamOperator;
import io.ray.streaming.operator.ISourceOperator;
import io.ray.streaming.operator.OneInputOperator;
import io.ray.streaming.operator.StreamOperator;
import io.ray.streaming.operator.TwoInputOperator;
import io.ray.streaming.operator.proxy.OneInputOperatorProxy;
import io.ray.streaming.operator.proxy.OutputOperatorProxy;
import io.ray.streaming.operator.proxy.SourceOperatorProxy;
import io.ray.streaming.operator.proxy.TwoInputOperatorProxy;
import io.ray.streaming.python.PythonOperator;
import io.ray.streaming.python.stream.PythonDataStream;
import io.ray.streaming.python.stream.PythonJoinStream;
import io.ray.streaming.python.stream.PythonKeyDataStream;
import io.ray.streaming.python.stream.PythonMergeStream;
import io.ray.streaming.python.stream.PythonUnionStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JobGraphBuilder {
  private static final Logger LOG = LoggerFactory.getLogger(JobGraphBuilder.class);

  private final int isJoinRightEdge = 1;

  private JobGraph jobGraph;
  private List<StreamSink> streamSinkList;
  private Set<IndependentOperatorDescriptor> independentOperatorDescriptors;

  public JobGraphBuilder(List<StreamSink> streamSinkList) {
    this(streamSinkList, new HashSet<>(), "job_" + System.currentTimeMillis());
  }

  public JobGraphBuilder(List<StreamSink> streamSinkList, String jobName) {
    this(streamSinkList, new HashSet<>(), jobName);
  }

  public JobGraphBuilder(
      List<StreamSink> streamSinkList,
      Set<IndependentOperatorDescriptor> independentOperatorDescriptors,
      String jobName) {
    this(streamSinkList, independentOperatorDescriptors, jobName, new HashMap<>());
  }

  public JobGraphBuilder(
      List<StreamSink> streamSinkList,
      Set<IndependentOperatorDescriptor> independentOperatorDescriptors,
      String jobName,
      Map<String, String> jobConfig) {
    this.jobGraph = new JobGraph(jobName, jobConfig);
    this.streamSinkList = streamSinkList;
    this.independentOperatorDescriptors = independentOperatorDescriptors;
  }

  public JobGraph build() {
    // setup DAG
    for (StreamSink streamSink : streamSinkList) {
      processStream(streamSink);
    }
    setupOperators();

    // setup independent operators
    setupIndependentOperators();

    return this.jobGraph;
  }

  @SuppressWarnings("unchecked")
  private void processStream(Stream stream) {
    while (stream.isProxyStream()) {
      // Proxy stream and original stream are the same logical stream, both refer to the
      // same data flow transformation. We should skip proxy stream to avoid applying same
      // transformation multiple times.
      LOG.debug("Skip proxy stream {} of id {}", stream, stream.getId());
      stream = stream.getOriginalStream();
    }
    AbstractStreamOperator streamOperator = stream.getOperator();
    Preconditions.checkArgument(
        stream.getLanguage() == streamOperator.getLanguage(),
        "Reference stream should be skipped.");
    int vertexId = stream.getId();
    int parallelism = stream.getParallelism();
    int dynamicDivisionNum = stream.getDynamicDivisionNum();
    JobVertex jobVertex;
    if (stream instanceof StreamSink) {
      jobVertex =
          new JobVertex(vertexId, parallelism, dynamicDivisionNum, VertexType.sink, streamOperator);
      Stream parentStream = stream.getInputStream();
      int inputVertexId = parentStream.getId();
      JobEdge jobEdge = new JobEdge(inputVertexId, vertexId, parentStream.getPartition());
      this.jobGraph.addEdgeIfNotExist(jobEdge);
      processStream(parentStream);
    } else if (stream instanceof StreamSource) {
      jobVertex =
          new JobVertex(
              vertexId, parallelism, dynamicDivisionNum, VertexType.source, streamOperator);
    } else if (stream instanceof DataStream || stream instanceof PythonDataStream) {
      if (stream instanceof JoinStream) {
        jobVertex =
            new JobVertex(
                vertexId, parallelism, dynamicDivisionNum, VertexType.join, streamOperator);
      } else if (stream instanceof UnionStream) {
        jobVertex =
            new JobVertex(
                vertexId, parallelism, dynamicDivisionNum, VertexType.union, streamOperator);
      } else {
        jobVertex =
            new JobVertex(
                vertexId, parallelism, dynamicDivisionNum, VertexType.process, streamOperator);
      }

      Stream parentStream = stream.getInputStream();
      int inputVertexId = parentStream.getId();
      JobEdge jobEdge = new JobEdge(inputVertexId, vertexId, parentStream.getPartition());
      this.jobGraph.addEdgeIfNotExist(jobEdge);
      processStream(parentStream);

      // process union stream
      List<Stream> streams = new ArrayList<>();
      if (stream instanceof UnionStream) {
        streams.addAll(((UnionStream) stream).getUnionStreams());
      }
      if (stream instanceof PythonUnionStream) {
        streams.addAll(((PythonUnionStream) stream).getUnionStreams());
      }
      for (Stream otherStream : streams) {
        JobEdge otherEdge = new JobEdge(otherStream.getId(), vertexId, otherStream.getPartition());
        this.jobGraph.addEdgeIfNotExist(otherEdge);
        processStream(otherStream);
      }

      // Process java join stream.
      if (stream instanceof JoinStream) {
        DataStream rightStream = ((JoinStream) stream).getRightStream();
        JobEdge rightJobEdge =
            new JobEdge(rightStream.getId(), vertexId, rightStream.getPartition());
        rightJobEdge.setEdgeType(isJoinRightEdge);
        this.jobGraph.addEdgeIfNotExist(rightJobEdge);
        processStream(rightStream);
      }
      // Process python join stream.
      if (stream instanceof PythonJoinStream) {
        PythonKeyDataStream rightStream = ((PythonJoinStream) stream).getRightStream();
        this.jobGraph.addEdgeIfNotExist(
            new JobEdge(rightStream.getId(), vertexId, rightStream.getPartition()));
        processStream(rightStream);
      }
      // Process multiple input stream.
      if (stream instanceof PythonMergeStream) {
        List<PythonKeyDataStream> rightStreams = ((PythonMergeStream) stream).getRightStreams();
        for (PythonKeyDataStream rightStream : rightStreams) {
          this.jobGraph.addEdgeIfNotExist(
              new JobEdge(rightStream.getId(), vertexId, rightStream.getPartition()));
          processStream(rightStream);
        }
      }
    } else {
      throw new UnsupportedOperationException("Unsupported stream: " + stream);
    }
    if (!jobGraph.containJobVertex(jobVertex.getVertexId())) {
      this.jobGraph.addVertex(jobVertex);
    }
  }

  /**
   * Base on up/down stream information, create operator proxy to handle cross-lang serialization.
   *
   * <p>Wrap collectors to do custom type cross-language serialization. If data in stream has a
   * schema and downstream is heterogeneous language, we serialize data in proxy to fury row format.
   */
  private void setupOperators() {
    jobGraph
        .getJobVertices()
        .forEach(
            vertex -> {
              Set<JobEdge> inputEdges = jobGraph.getVertexInputEdges(vertex.getVertexId());
              Set<JobEdge> outputEdges = jobGraph.getVertexOutputEdges(vertex.getVertexId());
              Set<Language> inputLanguages =
                  inputEdges.stream()
                      .map(edge -> jobGraph.getVertex(edge.getSourceVertexId()).getLanguage())
                      .collect(Collectors.toSet());
              Set<Language> outputLanguages =
                  outputEdges.stream()
                      .map(edge -> jobGraph.getVertex(edge.getTargetVertexId()).getLanguage())
                      .collect(Collectors.toSet());
              Preconditions.checkArgument(
                  inputLanguages.size() <= 1, "All input streams must have same language");
              Preconditions.checkArgument(
                  outputLanguages.size() <= 1, "All output streams must have same language");
              Language inputLanguage =
                  inputLanguages.iterator().hasNext() ? inputLanguages.iterator().next() : null;
              Language outputLanguage =
                  outputLanguages.iterator().hasNext() ? outputLanguages.iterator().next() : null;
              final StreamOperator operator = vertex.getOperator();
              if (operator instanceof PythonOperator) {
                PythonOperator pythonOperator = (PythonOperator) operator;
                if (inputLanguage != null) {
                  // All Python Stream Input should have same schema.
                  // See `io.ray.streaming.python.PythonOperator.getInputSchema`
                  JobEdge edge = inputEdges.iterator().next();
                  JobVertex srcVertex = jobGraph.getVertex(edge.getSourceVertexId());
                  if (srcVertex.getOperator().hasSchema()) {
                    pythonOperator.setInputSchema(srcVertex.getOperator().getSchema());
                    LOG.info(
                        "Operator {} has input data schema {}",
                        operator,
                        srcVertex.getOperator().getSchema());
                  } else {
                    LOG.warn("Operator {} doesn't have input data schema", operator);
                  }
                }
                if (outputLanguage != null) {
                  JobEdge edge = outputEdges.iterator().next();
                  JobVertex targetVertex = jobGraph.getVertex(edge.getTargetVertexId());
                  if (targetVertex.getOperator().hasSchema()) {
                    pythonOperator.setSchema(targetVertex.getOperator().getSchema());
                  }
                }
              } else {
                OutputOperatorProxy operatorProxy = null;
                switch (operator.getOpType()) {
                  case SOURCE:
                    {
                      if (outputLanguage != null
                          && outputLanguage != Language.JAVA
                          && operator.hasTypeInfo()
                          && TypeInference.isBean(operator.getTypeInfo().getType())) {
                        operatorProxy = new SourceOperatorProxy((ISourceOperator) operator);
                        operatorProxy.setTypeInfo(operator.getTypeInfo());
                      }
                      break;
                    }
                  case ONE_INPUT:
                    {
                      // Union stream will make one input has more than 1 input edge.
                      OneInputOperatorProxy oneInputOperatorProxy =
                          new OneInputOperatorProxy((OneInputOperator) operator);
                      // All input edges should have same data types.
                      JobEdge edge = inputEdges.iterator().next();
                      JobVertex srcVertex = jobGraph.getVertex(edge.getSourceVertexId());
                      if (inputLanguage != Language.JAVA) {
                        if (TypeInference.isBean(
                            oneInputOperatorProxy.getInputTypeInfo().getType())) {
                          operatorProxy = oneInputOperatorProxy;
                        }
                        if (srcVertex.getOperator().hasTypeInfo()
                            && TypeInference.isBean(
                                srcVertex.getOperator().getTypeInfo().getType())) {
                          oneInputOperatorProxy.setInputTypeInfo(
                              srcVertex.getOperator().getTypeInfo());
                          operatorProxy = oneInputOperatorProxy;
                        }
                      } else {
                        oneInputOperatorProxy.disableInputDecode(true);
                      }
                      if (outputLanguage != null
                          && outputLanguage != Language.JAVA
                          && operator.hasTypeInfo()
                          && TypeInference.isBean(operator.getTypeInfo().getType())) {
                        oneInputOperatorProxy.setTypeInfo(operator.getTypeInfo());
                        operatorProxy = oneInputOperatorProxy;
                      } else {
                        oneInputOperatorProxy.disableOutputEncode(true);
                      }
                      LOG.info(
                          "Operator {} has input data typeinfo {}",
                          operator,
                          oneInputOperatorProxy.getInputTypeInfo());
                      break;
                    }
                  case TWO_INPUT:
                    {
                      Preconditions.checkArgument(inputEdges.size() == 2);
                      Iterator<JobEdge> iterator = inputEdges.iterator();
                      JobEdge leftEdge = iterator.next();
                      JobEdge rightEdge = iterator.next();
                      JobVertex leftVertex = jobGraph.getVertex(leftEdge.getSourceVertexId());
                      JobVertex rightVertex = jobGraph.getVertex(rightEdge.getSourceVertexId());
                      TwoInputOperator twoInputOperator = (TwoInputOperator) operator;
                      TwoInputOperatorProxy twoInputOperatorProxy =
                          new TwoInputOperatorProxy(twoInputOperator);
                      if (inputLanguage != Language.JAVA) {
                        if (TypeInference.isBean(
                            twoInputOperator.getLeftInputTypeInfo().getType())) {
                          operatorProxy = twoInputOperatorProxy;
                        } else {
                          if (leftVertex.getOperator().hasTypeInfo()
                              && TypeInference.isBean(
                                  leftVertex.getOperator().getTypeInfo().getType())) {
                            twoInputOperatorProxy.setLeftInputTypeInfo(
                                leftVertex.getOperator().getTypeInfo());
                            operatorProxy = twoInputOperatorProxy;
                          }
                        }
                        if (TypeInference.isBean(
                            twoInputOperator.getRightInputTypeInfo().getType())) {
                          operatorProxy = twoInputOperatorProxy;
                        } else {
                          if (rightVertex.getOperator().hasTypeInfo()
                              && TypeInference.isBean(
                                  rightVertex.getOperator().getTypeInfo().getType())) {
                            twoInputOperatorProxy.setRightInputTypeInfo(
                                rightVertex.getOperator().getTypeInfo());
                            operatorProxy = twoInputOperatorProxy;
                          }
                        }
                      } else {
                        twoInputOperatorProxy.disableInputDecode(true);
                      }
                      if (outputLanguage != null
                          && outputLanguage != Language.JAVA
                          && operator.hasTypeInfo()
                          && TypeInference.isBean(operator.getTypeInfo().getType())) {
                        twoInputOperatorProxy.setTypeInfo(operator.getTypeInfo());
                        operatorProxy = twoInputOperatorProxy;
                      } else {
                        twoInputOperatorProxy.disableOutputEncode(true);
                      }
                      LOG.info(
                          "Operator {} has left input data typeinfo {}",
                          operator,
                          twoInputOperatorProxy.getLeftInputTypeInfo());
                      LOG.info(
                          "Operator {} has right input data typeinfo {}",
                          operator,
                          twoInputOperatorProxy.getRightInputTypeInfo());
                      break;
                    }
                  default:
                    throw new UnsupportedOperationException(
                        "Unsupported type " + operator.getOpType());
                }
                if (operatorProxy != null) {
                  vertex.setOperator(operatorProxy);
                }

                if (operator.hasTypeInfo()) {
                  LOG.info(
                      "Operator {} has output data typeinfo {}", operator, operator.getTypeInfo());
                } else {
                  if (operator.getLanguage() == Language.JAVA) {
                    String tpl = "Operator {} for function {} doesn't have output data typeinfo";
                    if (outputLanguage != Language.JAVA) {
                      tpl +=
                          ". You should set typeinfo by calling `Stream.withType` "
                              + "or using a subclass which can capture typeinfo";
                    }
                    LOG.warn(tpl, operator, operator.getFunction());
                  } else {
                    LOG.warn("Operator {} doesn't have output data typeinfo", operator);
                  }
                }
                if (operator.hasSchema()) {
                  LOG.info("Operator {} has output data schema {}", operator, operator.getSchema());
                } else {
                  LOG.warn("Operator {} doesn't have output data schema", operator);
                }
              }
            });
  }

  private void setupIndependentOperators() {
    this.jobGraph.setIndependentOperators(this.independentOperatorDescriptors);
  }
}
