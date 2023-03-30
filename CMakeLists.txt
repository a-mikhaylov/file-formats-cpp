cmake_minimum_required(VERSION 3.5.0)
project(file-formats-cpp 
        LANGUAGES C CXX 
        VERSION 0.1.0)
set(CMAKE_CXX_STANDARD 11)
include_directories(${HDF5_INCLUDE_DIRS})
# include_directories(${CMAKE_SOURCE_DIR}/_DuckDB/)
# include(CTest)
# enable_testing()

# HDF5
find_package(h5cpp 0.5.2)
find_package(hdf5 CONFIG 1.10.4)
# Arrow Parquet
find_package(Arrow REQUIRED)
find_package(Parquet REQUIRED)
find_package(ArrowDataset REQUIRED)

add_executable(file-formats-cpp main.cpp)

find_package(Boost COMPONENTS system filesystem REQUIRED)
target_link_libraries(file-formats-cpp ${Boost_FILESYSTEM_LIBRARY})
target_link_libraries(file-formats-cpp ${Boost_SYSTEM_LIBRARY})
target_link_libraries(file-formats-cpp h5cpp hdf5::hdf5) 

target_link_libraries(file-formats-cpp Arrow::arrow_shared)
target_link_libraries(file-formats-cpp Parquet::parquet_shared)
target_link_libraries(file-formats-cpp ArrowDataset::arrow_dataset_shared)

target_link_libraries(file-formats-cpp ${CMAKE_SOURCE_DIR}/_libs/_DuckDB/libduckdb.so)

# target_link_libraries(file-formats-cpp PRIVATE hdf5::hdf5-static hdf5::hdf5_hl-static)
# 
# 
# set(CPACK_PROJECT_NAME ${PROJECT_NAME})
# set(CPACK_PROJECT_VERSION ${PROJECT_VERSION})
# include(CPack)
