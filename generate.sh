#!/bin/bash
# Product generation script for hdwx-tasc
# Created 21 November 2022 by Sam Gardner <stgardner4@tamu.edu>

if [ ! -d output/ ]
then
    mkdir output/
fi
if [ -f ../config.txt ]
then
    source ../config.txt
else
    condaEnvName="HDWX"
fi
if [ -f ../HDWX_helpers.py ]
then
    if [ -f ./HDWX_helpers.py ]
    then
        rm ./HDWX_helpers.py
    fi
    cp ../HDWX_helpers.py ./
fi
echo $condaRootPath
echo $condaEnvName
if [ -f $condaRootPath/envs/$condaEnvName/bin/python3 ]
then
    if [ -f aprs-lock.txt ]
    then
        pidToCheck=`cat aprs-lock.txt`
        if ! kill -0 $pidToCheck
        then
            echo "Opening APRS connection..."
            $condaRootPath/envs/$condaEnvName/bin/python3 tascAPRS.py &
            echo -n $! > aprs-lock.txt
        else
            echo "APRS locked"
        fi
    else
        echo "Opening APRS connection..."
        $condaRootPath/envs/$condaEnvName/bin/python3 tascAPRS.py &
        echo -n $! > aprs-lock.txt
    fi
    if [ -f wspr-lock.txt ]
    then
        pidToCheck=`cat wspr-lock.txt`
        if ! kill -0 $pidToCheck
        then
            echo "Fetching WSPR data..."
            $condaRootPath/envs/$condaEnvName/bin/python3 tascWSPR.py &
            echo -n $! > wspr-lock.txt
        else
            echo "Plotter locked"
        fi
    else
            echo "Fetching WSPR data..."
            $condaRootPath/envs/$condaEnvName/bin/python3 tascWSPR.py &
            echo -n $! > wspr-lock.txt
    fi
    if [ -f plotter-lock.txt ]
    then
        pidToCheck=`cat plotter-lock.txt`
        if ! kill -0 $pidToCheck
        then
            echo "Plotting..."
            $condaRootPath/envs/$condaEnvName/bin/python3 tascPlot.py &
            echo -n $! > plotter-lock.txt
        else
            echo "Plotter locked"
        fi
    else
            echo "Plotting..."
            $condaRootPath/envs/$condaEnvName/bin/python3 tascPlot.py &
            echo -n $! > plotter-lock.txt
    fi
    # if [ "$1" != "--no-cleanup" ]
    # then
    #     echo "Cleaning..."
    #     $condaRootPath/envs/$condaEnvName/bin/python3 cleanup.py
    # fi
fi