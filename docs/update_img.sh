#!/bin/sh
DIAGRAMS="distributed-database distributed-sender mailtrain mailtrain-database centralized-sender"

rm ./img/*

for diagram in $DIAGRAMS; do
    drawio -x -f png -o "./img/${diagram}.png" "./drawio/${diagram}"
done

echo "DONE!"
