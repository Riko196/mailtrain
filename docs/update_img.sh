#!/bin/sh
DIAGRAMS="distributed-database distributed-master-worker mailtrain-database master-worker shard"

rm ./img/*

for diagram in $DIAGRAMS; do
    drawio -x -f png -o "./img/${diagram}.png" "./drawio/${diagram}"
done

echo "DONE!"
