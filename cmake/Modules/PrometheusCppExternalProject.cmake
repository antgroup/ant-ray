# Prometheus external project
# target:
#  - prometheus-cpp_ep
# defines:
#  - PROMETHEUS_CPP_ROOT
#  - Prometheus_Cpp_INCLUDE_DIR
#  - Prometheus_Cpp_PUSH_LIBRARY

if (DEFINED ENV{RAY_PROMETHEUS_CPP_ROOT} AND EXISTS $ENV{RAY_PROMETHEUS_CPP_ROOT})
  set(Prometheus_Cpp_USE_STATIC_LIBS ON)
  set(PROMETHEUS_CPP_ROOT "$ENV{RAY_PROMETHEUS_CPP_ROOT}")
  message(STATUS "Find PROMETHEUS_CPP_ROOT: ${PROMETHEUS_CPP_ROOT}")
  set(Prometheus_Cpp_INCLUDE_DIR ${PROMETHEUS_CPP_ROOT}/include)
  set(Prometheus_Cpp_LIBRARY_DIR ${PROMETHEUS_CPP_ROOT}/lib)
  set(Prometheus_Cpp_PUSH_LIBRARY ${Prometheus_Cpp_LIBRARY_DIR}/${CMAKE_STATIC_LIBRARY_PREFIX}prometheus-cpp-push${CMAKE_STATIC_LIBRARY_SUFFIX})

  add_custom_target(prometheus-cpp_ep)
else()
  message(STATUS "Starting to build prometheus")
  set(Prometheus_Cpp_INSTALL_PREFIX ${CMAKE_CURRENT_BINARY_DIR}/external/prometheus-cpp-install)

  set(Prometheus_Cpp_INCLUDE_DIR ${Prometheus_Cpp_INSTALL_PREFIX}/include)
  set(PROMETHEUS_CPP_ROOT ${Prometheus_Cpp_INSTALL_PREFIX})
  set(Prometheus_Cpp_LIBRARY_DIR ${Prometheus_Cpp_INSTALL_PREFIX}/lib)
  set(Prometheus_Cpp_PUSH_LIBRARY ${Prometheus_Cpp_LIBRARY_DIR}/${CMAKE_STATIC_LIBRARY_PREFIX}prometheus-cpp-push${CMAKE_STATIC_LIBRARY_SUFFIX})

  set(Prometheus_Cpp_URL https://github.com/jupp0r/prometheus-cpp.git)
  set(Prometheus_Cpp_TAG 63a24a77398c2253155d3e30d88de2c234f2c675)
  set(Prometheus_Cpp_CMAKE_CXX_FLAGS "${EP_CXX_FLAGS} -fPIC")

  set(Prometheus_Cpp_BUILD_PRODUCTS ${Prometheus_Cpp_PUSH_LIBRARY})

  set(Prometheus_Cpp_USE_STATIC_LIBS ON)

  set(Prometheus_Cpp_CMAKE_ARGS -DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE}
    -DCMAKE_INSTALL_PREFIX=${Prometheus_Cpp_INSTALL_PREFIX}
    -DCMAKE_CXX_FLAGS_${UPPERCASE_BUILD_TYPE}=${Prometheus_Cpp_CMAKE_CXX_FLAGS}
    -DCMAKE_C_FLAGS_${UPPERCASE_BUILD_TYPE}=${EP_C_FLAGS} 
    -DCMAKE_CXX_FLAGS=${Prometheus_Cpp_CMAKE_CXX_FLAGS})

  ExternalProject_Add(prometheus-cpp_ep
    PREFIX external/prometheus-cpp
    GIT_REPOSITORY ${Prometheus_Cpp_URL}
    GIT_TAG ${Prometheus_Cpp_TAG}
    BUILD_BYPRODUCTS ${Prometheus_Cpp_BUILD_PRODUCTS}
    CMAKE_ARGS ${Prometheus_Cpp_CMAKE_ARGS})

endif ()
