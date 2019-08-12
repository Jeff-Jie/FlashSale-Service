/*
GRPC客户端异步调用：
    需要自己有一个线程去调用CompletionQueue的next来等待消息。
    完成队列中，用next取得的，就是服务器已经完成的请求。
    里面有：request和response，context（上下文）和status（状态码）。
*/



#include <iostream>
#include <memory>
#include <string>
#include <pthread.h>

#include <grpc++/channel.h>
#include <grpc++/client_context.h>
#include <grpc++/create_channel.h>
#include <grpc++/security/credentials.h>
#include <grpc/support/log.h>

#include "seckill.grpc.pb.h"
#include "mysqlpool.h"
#include "Md5.h"

#define  MYSQL_ADDRESS "localhost"
#define  MYSQL_USRNAME "root"
#define  MYSQL_PORT 3306
#define  MYSQL_USRPASSWORD "000000"
#define  MYSQL_USEDB "test"
#define  MYSQL_MAX_CONNECTCOUNT 3

#define  Random(x) (rand() % x) //rand()是产生随机int型数, %x的话就可以取0到x之间的随机整数了
#define  SALT "salt"
#define  NUM_THREADS 5 //线程个数，代表了客户端数量。代表了有多少个人来抢购。

int response_count = 0;
int success_count = 0;
std::mutex mtx;


using grpc::Channel;
using grpc::ClientAsyncResponseReader;
using grpc::ClientContext;
using grpc::CompletionQueue;
using grpc::Status;
using seckill::SeckillRequest;
using seckill::SeckillResponse;
using seckill::SeckillService;


class SeckillClient 
{
 public:
  
  //显示构造函数
  explicit SeckillClient(std::shared_ptr<Channel> channel)
      : stub_(SeckillService::NewStub(channel)) {}

  //seckill函数。
  std::string seckill(const std::string &usr_name,const std::string &usr_key) 
  {
    SeckillRequest request;
    request.set_usrname(usr_name);
    request.set_usrkey(usr_key);

    SeckillResponse response;
    ClientContext context;
    CompletionQueue cq;

    Status status;

    // 调用stub_->AsyncSayHello异步接口向服务器发送请求，请求会放到cq队列中
    std::unique_ptr<ClientAsyncResponseReader<SeckillResponse> > rpc(
        stub_->PrepareAsyncseckill(&context, request, &cq));

    rpc->StartCall();

    // 设置rpc完成时动作，结果参数，状态，和tag
    //（tag用来方便应用层能区分不同的请求，，虽然grpc内部有每个请求自己的uid，但是对于应用层却不能用，通过在这里设置一个tag，
    //可以方便的区别，因为异步的可能会多个请求通过rpc发出，接收到后都放到了CompletionQueue中，所以应用层要能区分，就要设置一个tag）
    rpc->Finish(&response, &status, (void*)1);
    void* got_tag;
    bool ok = false;

    // CompletionQueue的next会阻塞到有一个结果为止，
    //got_tag会设置成前面的tag，而ok会设置成代码是否成功的拿到了响应（ok与status不同，status是表示服务器的结果，而ok只是表示阻塞队列中Next的结果）
    GPR_ASSERT(cq.Next(&got_tag, &ok)); //Next不是从完成队列中随便取出一个已完成请求，而是取出对应got_tag标签的已完成请求。也就是发过去的和接收回来的对应上。

    GPR_ASSERT(got_tag == (void*)1);
    GPR_ASSERT(ok);

    if (status.ok()) 
      return response.result();
    else 
      return "RPC failed";
  }

 private:
  std::unique_ptr<SeckillService::Stub> stub_; //智能指针，且只允许自己指向该对象。独占式。也就是说每个客户都有自己的一个关于服务器的存根。
};

//检查是否存在重复订单：
bool containsDuplicate(vector<int>& nums) //传入的是，mysql的order_info表中的所有usrname 
{
    sort(nums.begin(), nums.end()); //先使用c++的sort函数，进行排序，从小到大排列
    for (int i = 0; i < nums.size() - 1; i++) 
    {
        // cout << nums.size() << endl;
        if (nums[i] == nums[i + 1]) //如果有相同的，表示有重复订单
            return true;
    }
    return false;
}

//检查抢购情况
bool checkSeckillInfo()
{
    bool isnormol = true;
    MysqlPool *mysql = MysqlPool::getMysqlPoolObject();
    mysql->setParameter(MYSQL_ADDRESS,MYSQL_USRNAME,MYSQL_USRPASSWORD,MYSQL_USEDB,MYSQL_PORT,NULL,0,MYSQL_MAX_CONNECTCOUNT);
    MYSQL *connection = mysql->getOneConnect();

    //检查抢到的数量
    if (success_count > 50)
    {
        isnormol = false;
        std::cout << "抢到过多商品" << std::endl;
    }

    //检查数据库中商品数量
    int total_count = 0;
    std::string selSql("SELECT g_totalcount FROM goods_info WHERE g_id ='10' ");
    if(mysql_query(connection, selSql.c_str()))
    {
        std::cout << "selSql was Error:" << mysql_error(connection);
    }
    else
    {
        MYSQL_RES *result = mysql_use_result(connection);
        MYSQL_ROW row;
        while((row = mysql_fetch_row(result)) != NULL)
        {
            if (mysql_num_fields(result) > 0) 
                total_count = atoi(row[0]);
        }
        if (result != NULL)
          mysql_free_result(result);
    }
    if (50 - total_count != success_count)
    {
        isnormol = false;
        std::cout << "商品数量不对" << std::endl;
    }

    //检查订单数量、检查订单是否重复、是否未注册用户抢到商品
    std::map<const std::string,std::vector<const char* > >  resul_map = mysql->executeSql("SELECT usr_name FROM order_info"); //从order_info订单中查询usr_name
    if(resul_map.count("usr_name")>0)
    {
        std::vector<const char*> names = resul_map["usr_name"];
        if (names.size() > 0)
        {
            std::vector<int> namesI;
            for (int i=0; i<names.size(); i++)
                namesI.push_back(std::atoi(names[i]));
            if (names.size() != success_count)
            {
                std::cout << "订单数量不对" << std::endl;
                isnormol = false;
            }

            if (containsDuplicate(namesI))
            {
                std::cout << "存在重复订单" << std::endl;
                isnormol = false;
            }
        }     
    }
    delete mysql;

    return isnormol; //是否正常
}

//客户端执行onSeckill函数，传入一个参数args，也就是哪个用户。
//比如传入的是names[2]，表示第3个用户，names[2]的值是一个int型数字。
void* onSeckill(void* args) //这个args是一个数字的地址。
{
    int i = *(int *)args; //要从形参转变为int型，本来传进来的就是一对应这个数字的地址。
    std::string prikey = std::to_string (i) + std::to_string (i) + SALT;
    MD5 iMD5;
    iMD5.GenerateMD5((unsigned char *)prikey.c_str(), strlen(prikey.c_str()));
    std::string md5_str(iMD5.ToString());
    std::string name = std::to_string(i);
    // std::cout << "send username: " << "[" << name << "]" << std::endl;
    //到这里，我们就得到了用户名和密码，分别是字符串的name,md5_str。   服务器那边用usr_name,usr_key来表示。

    //创建本地存根
    SeckillClient SeckillClient(grpc::CreateChannel("localhost:50051", grpc::InsecureChannelCredentials())); //得到存根SeckillClient
    
    //调用grpc服务请求，该请求会放到cq_队列中。
    //seckill函数中，客户的grpc服务请求会放到cq队列中，同时阻塞等待，从cq完成队列中取出对应自己的请求的已完成请求。（而不是从中随便拿一个已完成请求。）
    std::string reply = SeckillClient.seckill(name,md5_str); //用存根去调用对应的服务函数。
    //这里通过本地存根直接调用方法，很方便，而不用去send啥的，调用远端进程的服务就像本地调用一样简单，这就是grpc。
    //传入两个参数name，md5_str，得到一个回复，用reply接收。
    //存了的两个参数，usr_name和prikey生成的16字节字符串分别是key和value，用于redis的。

    //到这里，我们就已经从cq队列中拿到了自己发过去的请求，的已完成请求的相关回复了。


    // std::cout << "seckill received: " << reply << std::endl;
    int resI = atoi(reply.c_str()); //服务器那边，执行seckill失败，就会返回"0"，成功就会返回"1"。

    mtx.lock();
    if (resI == 1) //如果seckill成功，那么成功个数加1.
        ++ success_count;
    ++ response_count; //不管成功与否，有返回值，都代表一个客户执行seckill完成
    mtx.unlock();

    if (response_count == 3) //这里应该对应NUM_PTHREADS
    {
        if (checkSeckillInfo())
            std::cout << "秒杀结束，一切正常！一共抢到："<< success_count << "件商品！" << std::endl;
        else
            std::cout << "秒杀结束，出现错误！！！一共抢到："<< success_count << "件商品！" << std::endl;
    }
    // pthread_detach(pthread_self());
    return ((void *)0);
}


//我现在有NUM_THREADS个用户来抢购，那么这个函数用来设置这些用户都是什么类型的
void prepareTestData(int testType,int data[])
{
    if (testType == 1)//全是注册用户,注册用户名为1~400
    {
        std::cout << "模拟全是注册用户"<< std::endl; //所谓注册用户，是指服务器的mysql和redis中有关于该用户的注册信息。可以进行服务。
        for (int i = 1; i < NUM_THREADS + 1; ++i) //data数组，里面所有的元素值都是400内的，而400内的都是注册的用户。（服务器那边注册了400个用户）
            data[i-1] = Random(400); 

    }else if(testType == 2)//模拟全部不是注册用户
    {
        std::cout << "模拟全不是注册用户"<< std::endl;
        for (int i = 1; i < NUM_THREADS + 1; ++i) //data数组里面的多有元素值都是400以外的，自然全部不是注册的用户。
            data[i-1] = Random(400) + 400;
        
    }else if(testType == 3)//模拟单一注册用户大量请求
    {
        std::cout << "模拟单一注册用户大量请求"<< std::endl;
        for (int i = 1; i < NUM_THREADS + 1; ++i) //data数组里面的元素值，范围都在0-5之间，那么就会出现单个注册用户，很多个请求
            data[i-1] = Random(5);

    }else if(testType == 4)//模拟单一未注册用户大量请求
    {
        std::cout << "模拟单一未注册用户大量请求"<< std::endl;
        for (int i = 1; i < NUM_THREADS + 1; ++i)
            data[i-1] = Random(5) + 400;
            
    }else if(testType == 5)//模拟部分注册用户，部分未注册用户
    {
        std::cout << "模拟部分注册用户，部分未注册用户"<< std::endl;
        for (int i = 1; i < NUM_THREADS + 1; ++i)
            data[i-1] = Random(600);
    }
}


int main(int argc, char** argv) 
{
    //<1>共有NUM_THREADS个用户来抢购
    //names[0]表示第1个用户，names[5]表示第6个用户...
    int names[NUM_THREADS] = {0}; //names数组是客户端，总共有NUM_THREADS个用户来抢购商品。

    //<2>设置这些用户类型
    if (argc > 0)
    {
        int testType = std::atoi(argv[argc-1]);
        prepareTestData(testType,names); //
    }
    
    //<3>需要开启NUM_THREADS个线程。（即每个用户需要一个线程，用来执行grpc请求调用服务，即线程函数onSeckill）
    pthread_t tids[NUM_THREADS];

    for(int i = 0; i < NUM_THREADS; ++i)
    {
        //参数依次是：创建的线程id，线程参数，调用的函数，传入的函数参数
        int ret = pthread_create(&tids[i], NULL, onSeckill, &names[i]); //注意，names是数组，names[i]是数组中的第i+1个元素，是一个数值。
        if (ret != 0)
            std::cout << "pthread_create error: error_code=" << ret << std::endl;
    }
    
    //<4>等各个线程退出后，进程才结束，否则进程强制结束了，线程可能还没反应过来；
    pthread_exit(NULL);
    return 0;
}

//服务器首先指定了NUM_THREADS个用户。
//然后对这些用户设置不同的类型。
//接下来开启NUM_THREADS个线程，这些线程都执行onSerckill函数，也就是抢购商品。（模拟多用户并发抢购商品）
//onSeckill函数中，根据传进来的用户id，得到usr_name和usr_key
//然后通过创建SeckillClient类对象，来创建本地存根，也就是服务器在客户端的投影一样。
//之后通过本地存根，调用类中的seckill函数，
//该函数中，客户端将服务请求放到cq队列中，然后对应好标签tag
//之后，根据该标签tag，阻塞等待从cq队列中取出对应的已完成请求。
//通过对回复response进行分析，得到相关的信息，可以判断是否成功。
//之后onSeckill函数中，判断reply是"1"还是"0"，就可以知道seckill抢购是否成功。
//然后每个onSeckill函数中，都会统计全局变量success_count; response_count;
//也就是秒杀成功的个数，以及执行完成的用户数。
//当执行完成的用户数response_count = 我们指定的用户总数时，秒杀结束，所有的用户发送的秒杀请求都已经处理完毕。（服务器也是开多线程处理的。）
//所有的用户的秒杀请求都处理完了，还要对这些抢购情况进行一个分析，判断中间是否出错，秒杀了多少个商品之类的输出信息。
//接下来，所有线程退出，回收资源，结束！！！