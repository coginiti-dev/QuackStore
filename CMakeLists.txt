cmake_minimum_required(VERSION 3.5)

# Set extension name here
set(TARGET_NAME quackstore)
project(${TARGET_NAME})

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED True)

set(EXTENSION_NAME ${TARGET_NAME}_extension)
set(LOADABLE_EXTENSION_NAME ${TARGET_NAME}_loadable_extension)

set(EXTENSION_INCLUDE_DIR "${CMAKE_CURRENT_SOURCE_DIR}/src/include")

include_directories(
  ${EXTENSION_INCLUDE_DIR}
)

file(GLOB_RECURSE EXTENSION_SOURCES "${CMAKE_CURRENT_SOURCE_DIR}/src/*.cpp")

if(NOT CLANG_TIDY)
  if(${BUILD_UNITTESTS})
    # Add tests
    add_subdirectory(test/unittest)

    if(NOT WIN32 AND NOT SUN AND ${BUILD_BENCHMARKS})
      # Add benchmarks
      # add_subdirectory(benchmark) # disabled for now
    endif()
  endif()

endif()

set(PARAMETERS "-warnings")

build_static_extension(${TARGET_NAME} ${EXTENSION_SOURCES})
target_include_directories(${EXTENSION_NAME} PUBLIC
    $<BUILD_INTERFACE:${EXTENSION_INCLUDE_DIR}>
    $<INSTALL_INTERFACE:include>  
)

build_loadable_extension(${TARGET_NAME} ${PARAMETERS} ${EXTENSION_SOURCES})
target_include_directories(${LOADABLE_EXTENSION_NAME} PUBLIC
    $<BUILD_INTERFACE:${EXTENSION_INCLUDE_DIR}>
    $<INSTALL_INTERFACE:include>
)


install(
  TARGETS ${EXTENSION_NAME} ${LOADABLE_EXTENSION_NAME}
  EXPORT "${DUCKDB_EXPORT_SET}"
  LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
  ARCHIVE DESTINATION "${INSTALL_LIB_DIR}"
)