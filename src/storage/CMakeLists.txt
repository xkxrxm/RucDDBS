set(STORAGE_DIR ${PROJECT_SOURCE_DIR}/include/storage/)
aux_source_directory(${PROJECT_SOURCE_DIR}/src/storage STORAGE_SRC)

add_library(storage STATIC ${STORAGE_SRC})
target_include_directories(storage PUBLIC ${STORAGE_DIR})
target_link_libraries(storage snappy)
    