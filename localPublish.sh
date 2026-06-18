#! /bin/bash

PACKAGE_NAME=rainman-1.0.0-py3-none-any.whl
LOCAL_WHEELS=./localWheels

mkdir -p $LOCAL_WHEELS

rm -f ${LOCAL_WHEELS}/${PACKAGE_NAME}

cp -p dist/${PACKAGE_NAME} ${LOCAL_WHEELS}