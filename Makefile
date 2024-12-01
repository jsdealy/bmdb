# bmdb:
# 	g++ -std=c++23 buildMDB.cpp -O3 -lfmt -Wall -o bmdb
bmdbsql:
	g++ -std=c++23 buildMDBsql.cpp -O3 -lSQLiteCpp -lsqlite3 -Wall -o bmdbsql
