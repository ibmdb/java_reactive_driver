#!/bin/bash

set -x

mvn dependency:copy-dependencies -DoutputDirectory=extlib
