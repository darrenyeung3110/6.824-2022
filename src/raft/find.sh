#!/bin/bash

search_string="FAIL"

# Iterate through files ending with .log in the current directory
for file in *.log; do
  # Check if the file contains the specific string
  if grep -q "$search_string" "$file"; then
    echo "$file"
  fi
done

