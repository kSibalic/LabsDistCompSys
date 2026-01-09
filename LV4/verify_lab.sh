#!/bin/bash

function run_assignment() {
    DIR=$1
    NAME=$2
    echo "==========================================="
    echo "   Running $NAME ($DIR)"
    echo "==========================================="
    
    cd $DIR
    make clean > /dev/null
    if make; then
        echo "Compilation Success."
        echo "To run this assignment:"
        echo "1. Open Terminal 1: cd $DIR && ./leader"
        echo "2. Open Terminal 2: cd $DIR && ./follower 1"
        echo "3. Open Terminal 3: cd $DIR && ./client"
    else
        echo "Compilation Failed!"
    fi
    cd ..
    echo ""
}

run_assignment "Assignment_1_CP" "Assignment 1: CP System"
run_assignment "Assignment_2_AP" "Assignment 2: AP System"
run_assignment "Assignment_3_Bonus" "Assignment 3: Conflict Resolution"
