# BUILD

mkdir bin && cd bin
cmake .. && make

# RUN
Init db from [sql](https://github.com/fourseaLee/ethdb/blob/master/doc/db.sql)
Edit you configure [file](https://github.com/fourseaLee/ethdb/blob/master/conf/conf.json)
cd bin && ./ethdb  or
./ethdb -c <configure file> like this
./ethdb -c ../conf/conf.json


