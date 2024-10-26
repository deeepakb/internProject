src := $(call read_sources_list,ddm.txt)
$(call cc_library, ddm.a, $(src), ddm)
