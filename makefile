subdir = ./

export PKG_CONFIG_PATH=/usr/local/lib/pkgconfig
SOURCES = $(wildcard $(subdir)*.cc)
SRCOBJS = $(patsubst %.cc,%.o,$(SOURCES))
CC = g++ -g

%.o:%.cc
	$(CC) -std=c++11 -I./ -I/usr/local/include -pthread -c $< -o $@

%.o:%.cpp
	$(CC) -std=c++11 -I./ -I./ -I/usr/local/include -pthread -c $< -o $@

all: seckill_client seckill_server

seckill_client:  seckill.grpc.pb.o seckill.pb.o Md5.o mysqlpool.o seckill_client.o
	$(CC) $^ -L/usr/local/lib -lgrpc++ -lgrpc  -Wl,--no-as-needed -lgrpc++_reflection -lprotobuf -lmysqlclient -lhiredis -lpthread -o $@

seckill_server:  seckill.grpc.pb.o seckill.pb.o Md5.o mysqlpool.o Worker.o redispool.o seckill_server.o 
	$(CC) $^ -L/usr/local/lib -lgrpc++ -lgrpc -Wl,--no-as-needed -lgrpc++_reflection -lprotobuf -lmysqlclient -lhiredis -lpthread -o $@

clean:
	rm *.o
