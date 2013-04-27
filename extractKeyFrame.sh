#!/bin/bash
echo $1 >> hello
filename=${1##*/}
echo $filename >> hello
keyFrameDir=output/dataset/${filename}/keyFrame/
echo $keyFrameDir
echo $keyFrameDir >> hello
hadoop fs -mkdir "$keyFrameDir"
hadoop fs -mkdir jiang
ffmpeg -i "$1" -vf select="eq(pict_type\,I)" -vsync 2 -s 300x225 "${filename}_%02d.jpg"
hadoop fs -put "${filename}_"*.jpg "$keyFrameDir"
rm ${filename}_*.jpg

