#!/bin/bash

# Requirements:
#   - Have Java 11+ installed
#   - Be at root directory

rm -rf *.go *.interp *.tokens
cd grammar/
java -Xmx500M -cp "$(printf %s: /usr/local/lib/antlr-*-complete.jar):$CLASSPATH" org.antlr.v4.Tool -Dlanguage=Go -package sqliteparser -o .. SQLiteLexer.g4 SQLiteParser.g4
cd ../
