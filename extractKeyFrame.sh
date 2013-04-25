#!/bin/bash
echo $1
filename=${1##*/}
echo $filename
echo $1
keyFrameDir=output/dataset/$filename/keyFrame/
echo $keyFrameDir
hadoop fs -mkdir $keyFrameDir
ffmpeg -i $1 -vf select="eq(pict_type\,I)" -vsync 2 -s 300x225 ${filename}_%02d.jpg
hadoop fs -put ${filename}_*.jpg $keyFrameDir
rm ${filename}_*.jpg

