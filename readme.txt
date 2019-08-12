./seckill.sh
make
去启动redis ./redis_server
./seckill_server
./serkill_client n



介绍：
protobuf：是谷歌自定义的序列化格式，类似与json，xml

grpc：一个高性能、开源和通用的 gooogle开发RPC 框架，基于 HTTP/2 设计。
	  可以使用grpc取代http用与移动端和后台服务器之间的通信，还有服务与服务之间的通信。
	  这样对于互联网公司来说，即减少了开发难度，又提高了效率。
	  （因为我们不需要再关心通信问题，同时不同的服务可以部署于不同的机器间，方便了分布式）