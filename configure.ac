dnl MemcachedbNG 
AC_PREREQ(2.59)
AC_INIT([MemcachedbNG],[1.0.0],[Server],[MichaelSong])
AC_CONFIG_AUX_DIR(config)
AM_INIT_AUTOMAKE([dist-bzip2])
AC_PREFIX_DEFAULT(/usr/local)
AC_CONFIG_HEADERS(inc/autoconf.h)

dnl
dnl check for programs 
dnl
AC_LIBTOOL_DLOPEN
AC_PROG_MAKE_SET
AC_PROG_CC
AC_PROG_CXX
AC_PROG_CXXCPP
AM_PROG_GCJ
AC_PROG_LIBTOOL
AC_PROG_INSTALL
AC_PROG_AWK
AC_CHECK_PROGS(MAKE,make gmake)
AC_CHECK_PROGS(AR,ar gar)
AC_CHECK_PROGS(GZIP,gzip)
AC_CHECK_PROG(HAVE_MAN2HTML,man2html,yes)
AM_PROG_CC_C_O

AC_MSG_CHECKING([check for debug compilation])
CFLAGS="-g -O2 -I./inc"
CXXFLAGS="-g -O2 -I./inc"

AC_MSG_CHECKING([uname -m for detecting host operating system architecture])
ARCH=`uname -m`
case "$ARCH" in
	x86_64*)
		CFLAGS="$CFLAGS -Wall -Wno-comment -m64 -fno-strict-aliasing"
		CXXFLAGS="$CXXFLAGS -Wall -Wno-comment -m64 -fno-strict-aliasing"
		AM_CONDITIONAL(TEST_64BIT, test "xyes" = "xyes")
	;;
	*)
		CFLAGS="$CFLAGS -Wall -Wno-comment -march=i686 -mtune=pentium4 -fno-strict-aliasing"
		CXXFLAGS="$CXXFLAGS -Wall -Wno-comment -march=i686 -mtune=pentium4 -fno-strict-aliasing"
		AM_CONDITIONAL(TEST_64BIT, test "xno" = "xyes")
	;;
esac
CFLAGS="$CFLAGS"
CXXFLAGS="$CXXFLAGS"

dnl
dnl check for header files, functions and typedefs...
dnl
AC_HEADER_STDC
AC_HEADER_DIRENT
AC_HEADER_TIME
AC_CHECK_HEADERS(unistd.h)
AC_CHECK_HEADERS(sys/time.h)
AC_CHECK_FUNCS(glob)
AC_LANG(C)
AC_LANG(C++)

dnl
dnl define private install directories 
dnl
AC_SUBST(sys_etcdir)
AC_SUBST(ld_so_confdir)
sys_etcdir=/etc
ld_so_confdir=/etc/ld.so.conf.d

dnl
dnl configure sub directories
dnl
AC_CONFIG_FILES([Makefile
		doc/Makefile
		tools/Makefile
		])
AC_OUTPUT
