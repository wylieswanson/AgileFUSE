bin_PROGRAMS = lama-readerd

lama_readerd_SOURCES = src/lama-readerd.c
lama_readerd_CFLAGS = -Wall

lama_readerd_LDFLAGS = -lpthread -ljson
lama_readerd_LDFLAGS += $(shell curl-config --libs)
