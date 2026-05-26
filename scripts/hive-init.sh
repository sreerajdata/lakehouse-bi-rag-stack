#!/bin/bash
HIVE_LIB=/opt/hive/lib
S3A_JAR="$HIVE_LIB/hadoop-aws-3.3.4.jar"
SDK_JAR="$HIVE_LIB/aws-java-sdk-bundle-1.12.262.jar"

if [ ! -f "$S3A_JAR" ]; then
  echo "Downloading hadoop-aws JAR..."
  curl -L https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar \
    -o "$S3A_JAR"
fi

if [ ! -f "$SDK_JAR" ]; then
  echo "Downloading aws-java-sdk JAR..."
  curl -L https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar \
    -o "$SDK_JAR"
fi

echo "Creating /tmp/hive with open permissions..."
mkdir -p /tmp/hive && chmod 777 /tmp/hive

exec /entrypoint.sh
