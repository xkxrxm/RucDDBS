set(ENGINE_DIR ${PROJECT_SOURCE_DIR}/include/engine/)
set(PARSER_DIR ${PROJECT_SOURCE_DIR}/include/parser/)
# 指定生成目标 
add_library(engine STATIC ${ENGINE_SRC})
target_include_directories(engine PUBLIC ${ENGINE_DIR} ${PARSER_DIR} ${TRANSACTION_DIR})
target_link_libraries(engine proto storage transaction ${BRPC_LIB} ${DYNAMIC_LIB})
