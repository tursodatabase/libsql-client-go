# ANTLR SQLite Parser

This project is just a generation of a [ANTLR4](https://www.antlr.org/) go parser using [SQLite grammar](https://github.com/antlr/grammars-v4/tree/master/sql/sqlite) from [antlr/grammars-v4](https://github.com/antlr/grammars-v4/tree/master) project. Its only reason to exist is to avoid forcing everyone that want a simple parser to install JAVA.

You can find the grammar inside `./grammar` and the generated parser inside `./sqliteparser`

## Generate parser

### Requirements
- JAVA 11+ installed
- ANTLR4 compelte java binaries. Link [here](https://www.antlr.org/download.html)
    - Put the binary inside `/usr/local/lib`

Important: The parser inside `./sqliteparser` was generated using OpenJDK 11 and ANTLR 4.12.0

### Steps
- Just run `./generate-parser.sh`

