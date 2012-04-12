#!/bin/bash
set -x

#make distclean

OS=`uname`
if [ $OS == 'Darwin' ]; then
	glibtoolize --force
else
	libtoolize --force
fi


#aclocal-1.9
aclocal
autoconf
autoheader

automake --add-missing
#automake-1.9 --add-missing
#automake-1.9
automake

#./configure $@
