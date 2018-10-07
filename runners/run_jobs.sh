#!/bin/bash

python -m luigi --module dag.pipeline HoroscopeReportTask --local-scheduler
