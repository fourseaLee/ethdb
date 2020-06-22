# BUILD

mkdir bin && cd bin
cmake .. && make

# RUN
Init db from [sql](https://github.com/SimbaBlock/sp/blob/master/xsvdb/doc/xsvdb.sql)
Edit you configure [file](https://github.com/SimbaBlock/sp/blob/master/xsvdb/conf/conf.json)
cd bin && ./xsvd  or
./xsvd -c <configure file> like this
./xsvd -c ../conf/conf.json


