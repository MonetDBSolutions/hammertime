#[[
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0.  If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright 1997 - July 2008 CWI, August 2008 - 2020 MonetDB B.V.
#]]

cmake_minimum_required(VERSION 3.12)

project(mthreaded VERSION 1.0)

set(C_STANDARD_REQUIRED ON)
set(CMAKE_C_STANDARD 99)

SET(CMAKE_INSTALL_RPATH_USE_LINK_PATH TRUE)

add_executable(mthreaded mthreaded.c)
add_executable(mthreaded2 mthreaded2.c)

find_package(MonetDB CONFIG REQUIRED)
find_package (Threads)

target_link_libraries(mthreaded
  PRIVATE
  MonetDB::monetdbe
  ${CMAKE_THREAD_LIBS_INIT}
)

target_link_libraries(mthreaded2
  PRIVATE
  MonetDB::monetdbe
  ${CMAKE_THREAD_LIBS_INIT}
)

install(TARGETS mthreaded mthreaded2)
