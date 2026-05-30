cmake_minimum_required(VERSION 3.10)
project(obcdc_rpc_test CXX)

set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# Compile options matching OceanBase's ABI settings
add_compile_options(-D_GLIBCXX_USE_CXX11_ABI=0 -w -Wno-reserved-user-defined-literal -Wno-error=reserved-user-defined-literal)
add_compile_definitions(__STDC_LIMIT_MACROS __STDC_CONSTANT_MACROS _NO_EXCEPTION OCI_LINK_RUNTIME)

# Define directories inside the container
set(OCEANBASE_DIR "/usr/local/code/oceanbase")

include_directories(
    ${OCEANBASE_DIR}
    ${OCEANBASE_DIR}/src
    ${OCEANBASE_DIR}/src/logservice/libobcdc/src
    ${OCEANBASE_DIR}/src/logservice/cdcservice
    ${OCEANBASE_DIR}/deps/oblib/src
    ${OCEANBASE_DIR}/deps/oblib/src/common
    ${OCEANBASE_DIR}/deps/easy/src
    ${OCEANBASE_DIR}/deps/easy/src/include
    ${OCEANBASE_DIR}/deps/ussl-hook
    ${OCEANBASE_DIR}/deps/ussl-hook/loop
    ${OCEANBASE_DIR}/src/objit/include
    ${OCEANBASE_DIR}/src/plugin/include
    ${OCEANBASE_DIR}/deps/3rd/usr/local/oceanbase/deps/devel/include
    ${OCEANBASE_DIR}/deps/3rd/usr/local/oceanbase/deps/devel/include/oss_c_sdk
    ${OCEANBASE_DIR}/deps/3rd/usr/local/oceanbase/deps/devel/include/libxml2
    ${OCEANBASE_DIR}/deps/3rd/usr/local/oceanbase/deps/devel/include/apr-1
    ${OCEANBASE_DIR}/deps/3rd/usr/local/oceanbase/deps/devel/include/icu
    ${OCEANBASE_DIR}/deps/3rd/usr/local/oceanbase/deps/devel/include/icu/common
    ${OCEANBASE_DIR}/deps/3rd/usr/local/oceanbase/deps/devel/include/mariadb
    ${OCEANBASE_DIR}/deps/3rd/usr/local/oceanbase/deps/devel/include/mxml
)

# Path to the compiled libobcdc.so inside the container
set(LIBOBCDC_SO "${OCEANBASE_DIR}/build_debug/src/logservice/libobcdc/src/libobcdc.so")

add_executable(obcdc_rpc_test main.cpp)

# Link against libobcdc.so
target_link_libraries(obcdc_rpc_test PRIVATE ${LIBOBCDC_SO})
