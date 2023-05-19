#!/bin/bash


file_path="./test.output"
search_string="exit"

# Loop 100 times
test_1() {
    > test.output
    for i in {1..100}; do
        # Run your command here
        go test -run TestReElection2A -race > test.output
        if grep -q "$search_string" "$file_path"; then
            break
        else
            echo "String not found in the file."
        fi
        # Print a new line after each run
    done
}

test_2() {
    > test.output
    for i in {1..100}; do
        go test -run 2A -race >> test.output
    done
}

test_2