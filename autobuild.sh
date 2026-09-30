#!/bin/bash

set -e
rm -rf `pwd`/bulid/*
cd `pwd`/build &&
    cmake .. &&
    make
cd ..
cp -r `pwd`/src/include `pwd`/lib