//完成队列：就是客户端将异步调用请求放到完成队列中，服务器从完成队列中取出请求进行处理
//          然后将处理后的响应放到完成队列，客户端再从完成队列中取出响应，这个完成队列，就是对于客户端来说，从里面取出的，已经是完成的服务。
/*
GRPC服务器异步：
    收到异步调用请求，
    首先是保存客户端传过来的上下文context（比如调用参数，client的writer），
    将该请求加入到完成队列中，
    在新的线程中，从队列中取出任务处理，
    调用writer向client写数据
*/





#include <memory> //提供c++11的智能指针，unique_ptr
#include <iostream>
#include <string>

#include "Worker.h" //包含了md5
#include "redispool.h"

#define  SERVER_ADDRESS "127.0.0.1:50051"

//grpc一次传输数据的最大允许字节长度：接收和发送都是5000Byte，一个char占1字节
//也就是说客户端调用服务，传递的请求服务数据中，最多可以有5000个char。足够放下账号密码什么的了。
//服务器的响应数据，最多可以有5000Byte。
//也可以不设置，grpc默认为4,194,304Byte = 4M.
//也就是说，我们反而把它设置小一点。。
#define  MAX_RECEIVE_SIZE 5000
#define  MAX_SEND_SIZE 5000

//work线程数。（开启多线程来处理消息队列中，客户端的服务请求）
#define  WORKER_NUM 20

/*
#define  MYSQL_ADDRESS "mymysql"
#define  MYSQL_USRNAME "tinykpwang"
#define  MYSQL_PORT 3306
#define  MYSQL_USRPASSWORD "14225117sa"
#define  MYSQL_USEDB "seckill"
#define  MYSQL_MAX_CONNECTCOUNT 30

#define  REDIS_ADDRESS "myredis"
#define  REDIS_PORT 6379
#define  REDIS_MAX_CONNECTCOUNT 500
*/

#define  MYSQL_ADDRESS "localhost" //连接本地（回环127.0.0.1）的mysql服务器
#define  MYSQL_USRNAME "root" //mysql登陆账号
#define  MYSQL_PORT 3306 //mysql默认端口号3306
#define  MYSQL_USRPASSWORD "000000" //mysql登陆密码
#define  MYSQL_USEDB "test" //使用mysql中的test数据库，相当于进入mysql之后，USE test;
#define  MYSQL_MAX_CONNECTCOUNT 30 //mysql最大连接数，即：

#define  REDIS_ADDRESS "localhost" //连接本地得redis服务器。即：要连接得redis，是运行在本机的，不是其他ip对应的机器上的redis。
#define  REDIS_PORT 6379 //redis默认端口号3306，不是自己编的
#define  REDIS_MAX_CONNECTCOUNT 500 //redis最大连接数，即：




//编译proto文件得到的pb文件中，相关的命名空间
using grpc::Server;
using grpc::ServerAsyncResponseWriter;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::ServerCompletionQueue; //服务器完成队列
using grpc::Status;
using seckill::SeckillRequest;
using seckill::SeckillResponse;
using seckill::SeckillService;


//我们之前已经用proto写好了需要的message消息类型和服务service
//然后用protocol编译器得到了相关的pb文件
//也就是说，我们已经定义好了相关的消息和服务。
//接下来，grpc，就是要对这个服务中的方法，进行对应的具体代码实现。
//而这个服务，就是服务器开启一个服务，然后客户端可以像本地调用一样的使用这个服务。
//但是服务中，具体是做什么的，也就是服务中的具体要实现的，也就是服务中具体的服务方法。
//我们还没有实现。
//接下来，我们就在我们自己编写的seckill_server服务器代码中，去创建并启动该grpc，同时编写该服务中的所有方法的实现。

//等于server中，我们直接使用proto得到的框架，创建grpc服务器，同时实现我们要服务的方法。
//然后client就可以像本地调用一样的调用这个服务。我们的服务器不需要去管收发什么的。

//server现在只要使用proto得到的pb文件（这里面好多东西用，什么异步模式啊，消息队列啊，服务器构造类工具啊...），去实现对应的服务方法即可。
//我们不需要去管怎么接收客户端，接收数据，发送数据。

//也就是编译proto文件，得到server和client的代码框架。
//客户端可以直接调用这些生成的代码，向服务端发送请求并接收响应。
//而服务端则需要服务的实现者自己来定制对请求的处理逻辑，生成响应并发回给客户端。

//grpc的server中的该类，就是去实现proto文件中定义的service服务中的方法。也就是实现proto文件的service服务中的全部方法，里面暂时只有seckill秒杀服务。
//完整的服务方法的实现可以不写在这个类中，因为我们server端写好就行。完全可以调用其他的。
//这个类最主要的作用，就是要从proto得到的pb文件中，公有继承下来，这样就等于使用那个文件中的东西。
class ServerImpl final
{
 public:
    ~ServerImpl()
    {
        server_->Shutdown(); //Shutdown()是grpc文件中提供的函数，用于关闭
        cq_->Shutdown();
    }


    //服务器主调用函数。
    void Run(MysqlPool *mysql, RedisPool *redis) //传入mysql连接池和redis连接池
    {
        //<1>设置服务器参数
        std::string server_address(SERVER_ADDRESS);

		//使用ServerBuilder来创建并启动服务器
        ServerBuilder builder; //grpc提供了ServerBuilder，用于创建和启动服务器
        builder.AddListeningPort(server_address, grpc::InsecureServerCredentials()); //服务器的ip+port
        builder.RegisterService(&service_); //将异步服务类对象service_，注册到该服务器中。也就是在该服务器上运行这个seckill服务。
        //设置grpc一次传输数据的最大允许字节长度。单位为Byte。
        builder.SetMaxReceiveMessageSize(MAX_RECEIVE_SIZE); //5000Byte
        builder.SetMaxSendMessageSize(MAX_SEND_SIZE);

        //消息队列
        cq_ = builder.AddCompletionQueue(); //将该消息队列添加到服务器中。也就是在该服务器上有一个消息队列，用于存放客户端的服务请求。
        redis_ = redis;


        //<2>创建并启动服务器
        server_ = builder.BuildAndStart(); //前面设置好了服务器的各个参数，现在正式创建并启动该服务器。
        std::cout << "Server listening on " << server_address << std::endl;
        
        //<3>
        int failedCounter = 0; //入库失败的次数
        pthread_rwlock_t rwlock; //创建一个读写锁rwlock
        pthread_rwlock_init(&rwlock, NULL); //初始化该读写锁rwlock
        sucNum_ = 0; //秒杀成功的个数，初始为0
        

        //疑问：为何需要new一个Worker类对象？
        //调用Worker的构造函数，和使用new去创建，有何不同？为何要用new？
        new Worker(&service_, cq_.get(), mysql, redis, &failedCounter, &sucNum_, &rwlock);
        //这个Worker类对象，到底做了什么？



        //开启WORKER_NUM个线程处理rpc调用请求。
        pthread_t tids[WORKER_NUM];
        for ( int i = 0 ; i < WORKER_NUM; i++ )
        {
          int ret = pthread_create(&tids[i], NULL, ServerImpl::ThreadHandlerRPC, (void*)this); //创建线程，线程中执行的函数是ThreadHandlerRPC()。
          if (ret != 0) //如果创建线程失败：
              std::cout << "pthread_create error: error_code=" << ret << std::endl;
        }

        //等各个线程退出后，回收资源，进程才结束。 调用pthread_join后，如果该子线程没有运行结束，调用者主线程会被阻塞。
        //或者直接线程分离。pthread_detach，将该子线程的状态设置为分离的，如此一来，该子线程运行结束后会自动释放所有资源。主线程无需等待阻塞。
        for ( int i = 0 ; i < WORKER_NUM; i++ )
        {
            pthread_join(tids[0],NULL); //pthread_join函数阻塞，直到线程tid终止，然后回收已终止线程占用的所有存储器资源。
        }
    }
    

    //处理rpc调用seckill服务请求
    static void* ThreadHandlerRPC(void* lparam)
    {
        ServerImpl* impl = (ServerImpl*)lparam;
        impl->HandleRPCS();
        return ((void *)0);
    }


    //服务器开启后，最终是等待在这里，等待从cq完成队列中取出客户端的秒杀请求进行处理。
    void HandleRPCS() 
    {
        void* tag; //这个tag标签应该就是代表了完成队列中的一个请求服务
        bool ok;
        while (true) 
        {
            //如果秒杀成功的个数小于50个，（这里代表我们定义只有50个商品可以抢购）
            //开始为下一次秒杀做准备：
            //从redis连接池中取出一个redis连接
            if (sucNum_ < 50)
            {
                redisContext *redisconn = redis_->getOneConnect();
                int workingNum = 0;
                //执行命令："llen work_list",并得到redisserver的回复，放到reply中。
                //llen work_list 表示输出列表的长度，不存在则视为空列表，输出0，有几个value，就输出几。
                redisReply *reply = (redisReply *)redisCommand(redisconn, "llen %s", "work_list");
                if(reply != NULL && reply->type == REDIS_REPLY_INTEGER) //如果回复不为空，且类型为整数
                {
                    workingNum = reply->integer;
                    freeReplyObject(reply);

                    if (workingNum >= 50 - sucNum_) { //workingNum表示正在执行秒杀的redis连接数。
                        //是这个样子的。
                        //我们总共有50个商品可以抢购，然后前面判断了已经抢购的商品个数不能超过50个
                        //但是还需要判断正在执行抢购行为的redis连接，不能超过50-sucNum个。
                        //如果超过了，那就把当前获取的redis连接关闭掉。不需要了，也不能抢购了。
                        //既然没有得到redis连接，那自然就不用去下面从cq中取一个rpc调用请求进行处理了。
                        //直接continue到while继续循环，万一有抢购失败的，就可以继续抢购了。此时的workingNum数量就会符合要求。
                        redis_->close(redisconn);
                        continue;
                    }
                }
                redis_->close(redisconn);
            }

            //接下来，阻塞在这里，等待从cq完成队列中取出一个rpc调用请求
            GPR_ASSERT(cq_->Next(&tag, &ok));
            GPR_ASSERT(ok);
            static_cast<Worker*>(tag)->Proceed(); //对取出的这个rpc调用请求，又要开始经历一个完整的处理步骤。
            //首先是proceed的CREATE状态，然后是PROCESS状态。。。
        }
    }

    /*
    unique_ptr 实现独占式拥有（exclusive ownership）或严格拥有（strict ownership）概念，
    保证同一时间内只有一个智能指针可以指向该对象。
    你可以移交拥有权。它对于避免内存泄漏（resource leak）——如 new 后忘记 delete ——特别有用。
    */
    //完成队列
    std::unique_ptr<ServerCompletionQueue> cq_;  // 一个生产者消费者队列
    //这个完成队列是用来干吗的呢？暂时网上查找不到相关的资料。
    //我暂时的理解，客户端异步调用服务，然后往该队列中添加了seckill服务请求。
    //然后服务器这边，从完成队列中取出服务请求，并进行处理。
    //这个完成队列，应该称为消息队列更为恰当！（存储客户端的服务请求）

    
    //异步服务
    SeckillService::AsyncService service_; //异步服务类对象service_
    
    //Server服务器
    std::unique_ptr<Server> server_;

    //记录秒杀成功的个数
    int sucNum_ ;

    RedisPool *redis_; //redis连接池
};
//class ServerImpl end;



//服务器这边，为秒杀请求做的准备工作，包括在指定数据库中创建表
void prepareForSeckill(MysqlPool *mymysql,RedisPool *redis){
    MYSQL *connection = mymysql->getOneConnect();
    redisContext *redisconn = redis->getOneConnect();
    
    int total_count = 0;

    //删除老的订单假数据，在test数据库中创建新的表。（如果有老的表，就会删除它们）
    std::string creatGoodsTSql("CREATE TABLE IF NOT EXISTS goods_info(g_name VARCHAR(20) NOT NULL,g_id VARCHAR(20) NOT NULL,g_totalcount INT UNSIGNED NOT NULL,PRIMARY KEY (g_id))");
    mysql_query(connection, creatGoodsTSql.c_str()); //执行该命令

    
    std::string insertGoodsSql("INSERT INTO goods_info(g_name, g_id, g_totalcount) VALUES('BMW', '10', 50) ON DUPLICATE KEY UPDATE g_id = '10', g_totalcount = 50");
    mysql_query(connection, insertGoodsSql.c_str());

    
    std::string deleteOrderSql("DROP TABLE IF EXISTS order_info");
    mysql_query(connection, deleteOrderSql.c_str());
    
    //下面这条sql语句，是从当前数据库中查询所有表名
    //information_schema 是MySQL系统自带的数据库，提供了对数据库元数据的访问
    //information_schema这张数据表保存了MySQL服务器所有数据库的信息。如数据库名，数据库的表，表栏的数据类型与访问权限等。
    //即这台MySQL服务器上，到底有哪些数据库、各个数据库有哪些表，每张表的字段类型是什么，各个数据库要什么权限才能访问，
    //等等信息都保存在information_schema表里面。
    //information_schema.tables 指数据库中的表（information_schema.columns 指列）

    //所以下面这句话的意思就是：从information_schema.TABLES表中，找到当前数据库的表名为usr_info的表。
    //执行这条命令，就会找到这个usr_info表，但是，这个表是啥时候创建的？看后面。应该不能先创建，这里正常情况下是查找不到。
    std::string tableSql("SELECT table_name FROM information_schema.TABLES WHERE table_name ='usr_info'");
    if(mysql_query(connection, tableSql.c_str())) //该语句执行失败：
    // if(mysql_query(connection, tesetsql.c_str()))
    {
        std::cout << "Query TableInfo Error:" << mysql_error(connection);
        exit(1);
    }
    else //mysql_query语句执行成功：
    {
        /*
        mysql_store_result() 和 mysql_use_result() 函数。
        在使用 mysql_query() 进行一次查询后，一般要用这两个函数之一来把结果存到一个 MYSQL_RES * 变量中。 
        两者的主要区别是，mysql_use_result() 的结果必须“一次性用完”，也就是说用它得到一个 result 后，
            必须反复用 mysql_fetch_row() 读取其结果直至该函数返回 null 为止，否则如果你再次进行 mysql 查询，
            会得到 “Commands out of sync; you can't run this command now” 的错误。
        而 mysql_store_result() 得到 result 是存下来的，你无需把全部行结果读完，就可以进行另外的查询。
        比如你进行一个查询，得到一系列记录，再根据这些结果，用一个循环再进行数据库查询，就只能用 mysql_store_result() 。 
        */
        /*
        MYSQL_RES *result，是查询得到的整个结果集，有很多行
        MYSQL_ROW row; 是查询得到的一行数据
        */

        MYSQL_RES *result = mysql_use_result(connection); //整个结果集
        MYSQL_ROW row; //一行

        //根据上面的usr_info表还没有创建，所以得到的结果集result肯定是空的，从里面取出一行肯定也是空的。
        //按道理肯定会执行下面的if，但是为什么没有执行呢？
        //我知道了！！！
        //每次重启服务器，都是按照1-400创建如下所示的400个用户名和id。所以第一次创建就行了，没必要每次都创建。
        //所以除了第一次创建了usr_info这个表，后面启动服务器都没必要再执行这段了，也就是说上面会显示查询到了usr_info这个表，跳过这里。

        //往数据库中插入测试数据,模拟用户注册信息,存储用户名+md5(用户名+密码+盐)
        //如果时第一次启动服务器创建usr_info表，那么就会执行如下if，否则会发现取出的一行中！= null，不执行如下语句。
        if ((row = mysql_fetch_row(result)) == NULL)  //一次从结果集result中，提取出一行到row。然后逐行分析：
        {

            std::string createSql("CREATE TABLE usr_info(usr_name VARCHAR(100) NOT NULL,usr_key VARCHAR(100) NOT NULL,PRIMARY KEY (usr_name))");
            mysql_query(connection, createSql.c_str()); //创建usr_info表。
            
            std::string insertSql("INSERT INTO usr_info(usr_name,usr_key) VALUES ");
            //模拟用户名密码都是i
            for (int i = 1; i <= 400; i ++) 
            {
                //将用户名和密码和盐生成一个prikey，eg:i为2时，prikey为"22salt"，
                std::string prikey = std::to_string (i) + std::to_string (i) + SALT; 
                MD5 iMD5;
                //然后使用这个唯一的prikey，去生成唯一的128bit，即16字节的字符串。
                iMD5.GenerateMD5((unsigned char *)prikey.c_str(), strlen(prikey.c_str()));
                std::string md5_str(iMD5.ToString());
                //从而得到了一对key-value键值对，即prikey和md5_str。可以用于redis，作为数据缓存。
                //redis那边就通过key-value键值对来校验客户，只有用户名和密码都对了，再加上salt这个标记，使用md5生成的16字节的字符串才能和这个value匹配上。
                //redis缓存那边才能通过，这样redis就可以作为缓存校验客户，减少mysql数据库的访问。

                //注意！！！redis使用的key-value，key是usr_name，即i
                //！！！！！！！！！！！！       value是以prikey(用户名加密码加盐，"iisalt")生成的16字节的字符串，即md5_str。

                
                if (i == 400)  //最后一对数据就不能在后面加，逗号了。
                    insertSql =  insertSql + "('" + std::to_string (i) + "','" + md5_str + "')"; //insertSql('user','md5值')
                    //这个md5值，即usr_key，用于在redis的
                else 
                    insertSql =  insertSql + "('" + std::to_string (i) + "','" + md5_str + "'),";
            }
            
            std::cout << "插入的sql为: " << insertSql << std::endl;
            mysql_query(connection, insertSql.c_str()); //执行插入命令
        }
        mysql_free_result(result);
        
        
        //查询数据库,将用户信息load到redis
        //这个也是只需要在第一次启动服务器时，load到redis。
        std::string searchSql("SELECT * FROM usr_info"); //usr_info刚创建好，里面肯定没有数据，现在就是往里面添加数据

        if(mysql_query(connection, searchSql.c_str())) //该语句执行失败
        {
            std::cout << "Query Usr_Info Error:" << mysql_error(connection);
            exit(1);
        }
        else //返回0，执行成功
        {
            //取出查询到的数据
            MYSQL_RES *searchResult = mysql_use_result(connection); //查询到的整个结果集
            MYSQL_ROW searchrow; //一行
            while((searchrow = mysql_fetch_row(searchResult)) != NULL) //从结果集中取出一行
            {
                if (mysql_num_fields(searchResult) > 1)  //字段的个数。肯定要大于1，不然怎么弄field和value
                {
                    std::string name(searchrow[0]);
                    std::string key(searchrow[1]);
                    
                    redisReply *reply = NULL;
                    reply = (redisReply *)redisCommand(redisconn, "HMSET usr_info  %s %s", name.c_str(), key.c_str());
                    //同时将多个 field-value (域-值)对设置到哈希表 key 中。此命令会覆盖哈希表中已存在的域。
                    //如果 key 不存在，一个空哈希表被创建并执行 HMSET 操作。
                    //执行成功，返回OK
                    //此处，usr_info是哈希表key，后面的name和key是field-value

                    if(reply != NULL && reply->type == REDIS_REPLY_STATUS && (strcasecmp(reply->str,"OK") == 0)) //strcasecmp忽略大小写比较字符串是否相等
                    {
                        // std::cout << "redis set Usr_Info OK!" << std::endl;
                        freeReplyObject(reply);
                    }else
                    {
                        std::cout << "redis set count error ";
                        if (reply != NULL ) 
                        {
                            std::cout << "error message:" << redisconn->errstr << std::endl;
                            freeReplyObject(reply);
                        }
                        redisFree(redisconn);
                        return;
                    }
                }
            }
            
            mysql_free_result(searchResult);
        } 
    }
    
    //查询商品数量
    std::string sql("SELECT * FROM goods_info");
    if(mysql_query(connection, sql.c_str()))
    {
        std::cout << "Query GoodsCount Error:" << mysql_error(connection);
        exit(1);
    }
    else
    {
        MYSQL_RES *result = mysql_use_result(connection); // 获取结果集
        MYSQL_ROW row; 
        while((row = mysql_fetch_row(result)) != NULL) //从结果集中取出一行，eg：商品名字，商品id，商品数量。这就有三列
        {
            if (mysql_num_fields(result) > 1)  //返回结果集result中的字段数，也就是有几列。会返回3
                total_count = atoi(row[2]); //第三列，也就是商品数量。 字符串转换为int
        }
        // 释放结果集的内存
        mysql_free_result(result);
    }

    //创建order_info表
    std::string createTSql("CREATE TABLE IF NOT EXISTS order_info(usr_name VARCHAR(100) NOT NULL,usr_key VARCHAR(100) NOT NULL,goods_id VARCHAR(100) NOT NULL,PRIMARY KEY (usr_name))");
    mysql_query(connection, createTSql.c_str());
    

    redisReply *reply = NULL;
    std::string scount = std::to_string(total_count);
    reply = (redisReply *)redisCommand(redisconn, "SET %s %s", "total_count", scount.c_str()); //设置key-value对。商品数量
    if(reply != NULL && reply->type == REDIS_REPLY_STATUS && (strcasecmp(reply->str,"OK") == 0))
    {
        std::cout << "redis set count OK!" << std::endl;
        freeReplyObject(reply);
    }else
    {
        std::cout << "redis set count error ";
        if (reply != NULL ) 
        {
            std::cout << "error message:" << redisconn->errstr << std::endl;
            freeReplyObject(reply);
        }
        redis->close(redisconn);
        mymysql->close(connection);
        return;
    }

    reply = (redisReply *)redisCommand(redisconn, "GET %s", "total_count"); //返回key对应的value，string类型的。
    freeReplyObject(reply); //重复使用同一个redisReply对象时，每次用完都用释放
    
    redisCommand(redisconn, "DEL %s", "order_info"); //Redis DEL 命令用于删除已存在的键。不存在的 key 会被忽略
    //这条命令，是用来清空上一次启动服务器时，在redis中创建的哈希表类型的key键order_info。
    //因为上一次启动服务器，处理成功客户端的请求之后，就会创建一个订单order，就会有一个field-value放到哈希表keyorder_info中。
    //redis先作为缓存，保存下订单数据。
    //然后放到mysql的order_info表中，包括：usr_name,usr_key,goods_id。

    mymysql->close(connection);
    redis->close(redisconn);
    
}


int main(int argc, char** argv) 
{
  
    //初始化数据库
    MysqlPool *mysql = MysqlPool::getMysqlPoolObject();
    mysql->setParameter(MYSQL_ADDRESS,MYSQL_USRNAME,MYSQL_USRPASSWORD,MYSQL_USEDB,MYSQL_PORT,NULL,0,MYSQL_MAX_CONNECTCOUNT);

    //初始化redis
    RedisPool *redis = RedisPool::getRedisPoolObject();
    redis->setParameter(REDIS_ADDRESS,REDIS_PORT,NULL,0,REDIS_MAX_CONNECTCOUNT);

    //准备初始数据
    prepareForSeckill(mysql,redis); //这个准备初始数据，就是把mysql的用户信息load到redis，准备好秒杀
    //该函数做了如下事情：
    //通过获取一个mysql连接和redis连接。（该主线程先来操作mysql数据库和redis缓存，进行准备）
    //在mysql中，创建了三个表：usr_info用户数据表，goods_info商品数据表，order_info订单表（如果没有这些表才创建）
    //将mysql中usr_info用户信息（key:usr_name = id,value:pass = id+id+salt）和商品数量（key:total_count,value:scount在mysql中查到的）load到redis中。
    //所谓的load，就是执行redisCommand()函数，执行SET等命令。

    ServerImpl server;
    server.Run(mysql,redis);
  
    delete mysql;
    delete redis;

    return 0;
}

//服务器流程：
//<1>创建mysql连接池
//<2>创建redis连接池
//<3>准备工作：主线程从mysql和redis连接池中分别获取一个连接，操作mysql和redis。包括mysql中创建三个表，将usr_info和商品数量load到redis中。
//<4>创建一个ServerImpl类对象server。
//<5>Run，创建并启动grpc服务器，同时绑定一个cq_完成队列
//   开启多线程，从cq_完成队列中取出grpc调用请求服务进行处理。
//   每个请求服务处理流程需要执行如下步骤（状态）：CREATE,PROCESS,FINSH（从Worker类，封装了异步请求处理所需要的状态与逻辑（处理））
//   CREATE就是注册请求服务到cq队列，PROCESS就是处理，包括在redis中校验用户，查询商品数量等。FINSH就是处理完成，关闭连接。
//<6>delete主线程的mysql连接和redis连接。
//<7>OK！