set(METRICS_SRCS
  tag/tags.cc
  registry/metrics_registry_interface.cc
  registry/prometheus_metrics_registry.cc
  reporter/prometheus_push_reporter.cc
  group/metrics_group_interface.cc
  group/default_metrics_group.cc)

set(METRICS_LIB_DEPENDENCIES
  boost_ep
  prometheus-cpp_ep)

set(METRICS_LIB_STATIC_LINK_LIBS ${PROMETHEUS_CPP_CORE_LIBRARY} ${PROMETHEUS_CPP_PUSH_LIBRARY} ${Boost_SYSTEM_LIBRARY} ${Boost_THREAD_LIBRARY} ${Boost_DATA_TIME_LIBRARY})

ADD_RAY_LIB(metrics
  SOURCES ${METRICS_SRCS}
  DEPENDENCIES ${METRICS_LIB_DEPENDENCIES}
  STATIC_LINK_LIBS ${METRICS_LIB_STATIC_LINK_LIBS})

install(FILES
  perf_count.h
  metrics_conf.h
  tags/tags.h
  group/metrics_group_interface.h
  group/default_metrics_group.h
  registry/metrics_registry_interface.h
  registry/prometheus_metrics_registry.h
  registry/empty_metrics_registry.h
  reporter/metrics_reporter_interface.h
  reporter/prometheus_push_reporter.h
  reporter/empty_metrics_reporter.h
  DESTINATION "${CMAKE_INSTALL_INCLUDEDIR}/ray/metrics"
)
