# curl external project
# target:
#  - curl_ep
# defines:
#  - CURL_HOME
#  - CURL_INCLUDE_DIR
#  - CURL_STATIC_LIB

if(DEFINED ENV{RAY_CURL_HOME} AND EXISTS $ENV{RAY_CURL_HOME})
  set(CURL_HOME "$ENV{RAY_CURL_HOME}")
  set(CURL_INCLUDE_DIR "${CURL_HOME}/include")
  set(CURL_STATIC_LIB "${CURL_HOME}/lib/libcurl.a")

  add_custom_target(curl_ep)
else()
  message(STATUS "Starting to build curl")
  set(CURL_VERSION "7.60.0")
  set(CURL_VERSION_ALIAS "7_60_0")
  # keep the url md5 equals with the version, `md5 v1.3.0.tar.gz`
  set(CURL_URL_MD5 "48eb126345d3b0f0a71a486b7f5d0307")
  set(CURL_CMAKE_CXX_FLAGS "${EP_CXX_FLAGS} -fPIC")
  if(APPLE)
    set(CURL_CMAKE_CXX_FLAGS "${CURL_CMAKE_CXX_FLAGS} -mmacosx-version-min=10.12")
  endif()

  set(CURL_URL "https://github.com/curl/curl/releases/download/curl-${CURL_VERSION_ALIAS}/curl-${CURL_VERSION}.tar.gz")
  set(CURL_PREFIX "${CMAKE_CURRENT_BINARY_DIR}/external/curl-install")
  set(CURL_HOME "${CURL_PREFIX}")
  set(CURL_INCLUDE_DIR "${CURL_PREFIX}/include")
  set(CURL_STATIC_LIB "${CURL_PREFIX}/lib/libcurl.a")

  set(CURL_CMAKE_ARGS -DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE}
    -DCMAKE_INSTALL_PREFIX=${CURL_PREFIX}
    -DBUILD_TESTING=OFF
    -DCURL_STATICLIB=ON
    -DHTTP_ONLY=ON
    -DCURL_CA_PATH=none
    -DCURL_ZLIB=OFF
    -DCMAKE_USE_OPENSSL=OFF
    -DENABLE_THREADED_RESOLVER=OFF
    -DCMAKE_CXX_FLAGS_${UPPERCASE_BUILD_TYPE}=${CURL_CMAKE_CXX_FLAGS}
    -DCMAKE_C_FLAGS_${UPPERCASE_BUILD_TYPE}=${EP_C_FLAGS}
    -DCMAKE_CXX_FLAGS=${CURL_CMAKE_CXX_FLAGS})

  ExternalProject_Add(curl_ep
    PREFIX external/curl
    URL ${CURL_URL}
    URL_MD5 ${CURL_URL_MD5}
    BUILD_IN_SOURCE 1
    BUILD_BYPRODUCTS "${CURL_STATIC_LIB}"
    CMAKE_ARGS ${CURL_CMAKE_ARGS})
endif()
