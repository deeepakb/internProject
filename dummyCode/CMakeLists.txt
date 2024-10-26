read_sources_list(ddm_sources ddm.txt)
add_library(ddm ${ddm_sources})
add_dependencies(ddm copy-pg-headers generate-headers omnisql)
