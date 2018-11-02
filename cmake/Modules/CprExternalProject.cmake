# cpr external project
# target:
#  - cpr_ep
# defines:
#  - CPR_HOME
#  - CPR_INCLUDE_DIR
#  - CPR_STATIC_LIB

if(DEFINED ENV{RAY_CPR_HOME} AND EXISTS $ENV{RAY_CPR_HOME})
  set(CPR_HOME "$ENV{RAY_CPR_HOME}")
  set(CPR_INCLUDE_DIR "${CPR_HOME}/include")
  set(CPR_STATIC_LIB "${CPR_HOME}/lib/libcpr.a")

  add_custom_target(cpr_ep)
else()
  message(STATUS "Starting to build cpr")
  set(CPR_VERSION "1.3.0")
  # keep the url md5 equals with the version, `md5 v1.3.0.tar.gz`
  set(CPR_URL_MD5 "f9df0c649208b06dd314699b4eb43759")

  set(CPR_URL "https://github.com/whoshuu/cpr/archive/${CPR_VERSION}.tar.gz")
  set(CPR_PREFIX "${CMAKE_CURRENT_BINARY_DIR}/external/cpr-install")
  set(CPR_HOME "${CPR_PREFIX}")
  set(CPR_INCLUDE_DIR "${CPR_PREFIX}/include")
  set(CPR_LIB_DIR "${CPR_PREFIX}/lib")
  set(CPR_STATIC_LIB "${CPR_LIB_DIR}/libcpr.a")

  set(CPR_SOURCE_DIR "${CMAKE_CURRENT_BINARY_DIR}/external/cpr/src/cpr_ep")
  set(CPR_SOURCE_LIB_DIR "${CPR_SOURCE_DIR}/lib")
  set(CPR_SOURCE_INCLUDE_DIR "${CPR_SOURCE_DIR}/include")

  set(CPR_CMAKE_ARGS -DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE}
    #    -DCMAKE_INSTALL_PREFIX=${CPR_PREFIX}
    -DUSE_SYSTEM_CURL=ON
    -DBUILD_CPR_TESTS=OFF
    -DCMAKE_CXX_FLAGS_${UPPERCASE_BUILD_TYPE}=${CPR_CMAKE_CXX_FLAGS}
    -DCMAKE_C_FLAGS_${UPPERCASE_BUILD_TYPE}=${EP_C_FLAGS}
    -DCMAKE_CXX_FLAGS=${CPR_CMAKE_CXX_FLAGS})

  ExternalProject_Add(cpr_ep
    PREFIX external/cpr
    URL ${CPR_URL}
    URL_MD5 ${CPR_URL_MD5}
    BUILD_IN_SOURCE 1
    BUILD_BYPRODUCTS "${CPR_STATIC_LIB}"
    CMAKE_ARGS ${CPR_CMAKE_ARGS}
    INSTALL_COMMAND "")

  ExternalProject_Add_Step(cpr_ep cpr_ep_install_lib
    COMMAND bash -c "cp -arf ${CPR_SOURCE_LIB_DIR} ${CPR_HOME}"
    DEPENDEES build)
  ExternalProject_Add_Step(cpr_ep cpr_ep_install_include
    COMMAND bash -c "cp -arf ${CPR_SOURCE_INCLUDE_DIR} ${CPR_HOME}"
    DEPENDEES build)

endif()
