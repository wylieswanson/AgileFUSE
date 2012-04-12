#!/bin/bash

# Delete automatically generated autostuff and run autogen.sh

set -x

cd "$(dirname "$0")"


make distclean

# rm -vf INSTALL

rm -vf aclocal.m4
rm -vrf autom4te.cache
rm -vf compile
rm -vf config.guess
rm -vf config.h
rm -vf config.h.in
rm -vf config.log
rm -vf config.status
rm -vf config.status.lineno
rm -vf config.sub
rm -vf configure
rm -vf depcomp
rm -vf install-sh
rm -vf lmd-1.0.pc
rm -vf lmdapi.lsm
rm -vf ltmain.sh
rm -vf Makefile
rm -vf Makefile.in
rm -vf missing
rm -vf texinfo.tex


#bash autogen.sh $@
