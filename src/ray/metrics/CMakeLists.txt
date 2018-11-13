set(METRICS_SRCS
  tag/tags.cc
  registry/metrics_registry_interface.cc
  registry/prometheus_metrics_registry.cc
  reporter/prometheus_push_reporter.cc
  group/metrics_group_interface.cc
  group/default_metrics_group.cc
  perf_counter.cc
  metrics_conf.cc)

set(METRICS_LIB_DEPENDENCIES
  boost_ep
  prometheus-cpp_ep)

set(METRICS_LIB_STATIC_LINK_LIBS ${PROMETHEUS_CPP_PUSH_LIBRARY} ${PROMETHEUS_CPP_CORE_LIBRARY} ${Boost_SYSTEM_LIBRARY})

ADD_RAY_LIB(metrics
  SOURCES ${METRICS_SRCS}
  DEPENDENCIES ${METRICS_LIB_DEPENDENCIES}
  STATIC_LINK_LIBS ${METRICS_LIB_STATIC_LINK_LIBS})

install(FILES
  metrics_util.h
  metrics_conf.h
  perf_counter.h
  tag/tags.h
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

ADD_RAY_TEST(registry/prometheus_metrics_registry_test
  STATIC_LINK_LIBS metrics_static gtest gtest_main pthread gmock_main ${PROMETHEUS_CPP_CORE_LIBRARY} ${Boost_SYSTEM_LIBRARY})

ADD_RAY_TEST(reporter/prometheus_push_reporter_test
  STATIC_LINK_LIBS metrics_static gtest gtest_main pthread gmock_main ${PROMETHEUS_CPP_PUSH_LIBRARY} ${PROMETHEUS_CPP_CORE_LIBRARY} ${CURL_STATIC_LIB} ${Boost_SYSTEM_LIBRARY})

ADD_RAY_TEST(metrics_conf_test STATIC_LINK_LIBS metrics_static gtest gtest_main pthread gmock_main)

ADD_RAY_TEST(perf_counter_test STATIC_LINK_LIBS metrics_static gtest gtest_main pthread gmock_main
  ${PROMETHEUS_CPP_PUSH_LIBRARY} ${PROMETHEUS_CPP_CORE_LIBRARY} ${CURL_STATIC_LIB} ${Boost_SYSTEM_LIBRARY})

ADD_RAY_TEST(metrics_util_test STATIC_LINK_LIBS metrics_static gtest gtest_main pthread gmock_main)
