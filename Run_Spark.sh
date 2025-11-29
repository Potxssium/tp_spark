#!/bin/bash

COMPILATION="${1:-true}"
TESTS="${2:-false}"

# Compile if COMPILATION is true
echo "Compilation flag is set to: $COMPILATION"
if [ "$COMPILATION" = true ]; then
    echo "Compiling project..."
    mvn package
fi

echo "Starting Spark job..."

if [ "$TESTS" = true ]; then
    echo "Running tests..."
    mvn test
fi

spark-submit \
  --class Gold \
  target/tp_spark-job-1.0.0-SNAPSHOT-jar-with-dependencies.jar \
  data/silver \

echo "Spark job finished."