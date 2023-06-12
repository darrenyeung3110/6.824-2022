#!/bin/bash

destination_file="big.log"
counter=1

# Create an empty destination file or clear its contents if it already exists
> "$destination_file"

# Iterate through files ending with .log in the current directory
for file in *.log; do
  # Skip the destination file itself
  if [ "$file" != "$destination_file" ]; then
    # Append the contents of each file to the destination file
    #
    echo "File $counter:" >> "$destination_file"
    cat "$file" >> "$destination_file"
    echo >> "$destination_file"
    ((counter++))
  fi
done

