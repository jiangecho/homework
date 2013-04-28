#!/bin/bash
filename=${1##*/}
echo $filename >> hello
keyFrameDir=/output/dataset/${filename}/keyFrame/
audioDir=/output/dataset/${filename}/audio/
echo $keyFrameDir
echo $audioDir
hadoop fs -mkdir "$keyFrameDir"
hadoop fs -mkdir "$audioDir"
ffmpeg -i "$1" -vf select="eq(pict_type\,I)" -vsync 2 -s 300x225 "${filename}_%02d.jpg"
ffmpeg -i "$1" -ar 16K -ac 1 "${filename}.wav"
hadoop fs -put "${filename}_"*.jpg "$keyFrameDir"
hadoop fs -put "${filename}.wav"  "$audioDir"
rm "${filename}_"*.jpg

