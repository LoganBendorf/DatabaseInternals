#!/bin/bash
set -o pipefail
clear && unbuffer cmake --build build -j8 2>&1 | head -n 100
BUILD_RESULT=$?

beep_pair() {
  local freq_1=$1
  local duration_ms_1=$2
  local freq_2=$3
  local duration_ms_2=$4
  local duration_sec_1=$(echo "scale=3; $duration_ms_1/1000" | bc)
  local duration_sec_2=$(echo "scale=3; $duration_ms_2/1000" | bc)
  
  play -n synth $duration_sec_1 sine $freq_1 triangle $(( (freq_1 * 3) / 2 )) vol 0.12 2>/dev/null
  play -n synth $duration_sec_2 sine $freq_2 triangle $(( (freq_2 * 3) / 2 )) vol 0.12 2>/dev/null
}

if [ $BUILD_RESULT -eq 0 ]; then
  #beep_pair 330 130 587 130
  exit $BUILD_RESULT
else
  #beep_pair 220 100 186 140
  exit $BUILD_RESULT
fi