#include "Worker.h"
// #include <thread>
#include <mutex>

#include "redispool.h"

#define RETRYCOUNT 20

// bool hasStored = false;
std::mutex mtx;

//服务器采用多线程从cq中取出的grpc调用请求服务，都要经历Worker中的如下步骤：

// 这个类封装了异步请求处理所需要的状态与逻辑
Worker::Worker(SeckillService::AsyncService* service, ServerCompletionQueue* cq, MysqlPool *mysql,RedisPool *redis, int *failedCount,int *successNum, pthread_rwlock_t *rwlock)
        : service_(service), cq_(cq), responder_(&ctx_), status_(CREATE), mysql_(mysql),redis_(redis),failedCount_(failedCount),successNum_(successNum),rwlock_(rwlock)
{
      Proceed();
}

void Worker::Proceed()
{
    //<1>如果状态是CREATE
    if (status_ == CREATE){
        status_ = PROCESS; //设置下一个状态
        service_->Requestseckill(&ctx_, &request_, &responder_, cq_, cq_,this); //注册请求服务
        //我们要对rpc服务service_，将我们proto定义的服务seckill注册到完成队列中。
        //也就是后面该完成队列cq_，可以收发关于该服务方法的请求和处理。
    } 
    //<2>如果状态是PROCESS
    else if (status_ == PROCESS){
        //连接redis
        redisContext *redisconn = redis_->getOneConnect();
        MYSQL *mysqlconnection = mysql_->getOneConnect();
        status_ = FINISH;

        redisCommand(redisconn, "LPUSH work_list %s", "1"); //列表类型的key：work_list中，往左边插入value：1
        //每次获取一个redis连接，进行处理的时候，都会往左边插入一个"1"，这样server就可以判断，现在有work_list有几个字段，即1.从而判断正在执行抢购的数量。

        new Worker(service_, cq_, mysql_,redis_,failedCount_,successNum_,rwlock_);

        auto usr_name = request_.usrname();
        auto usr_key = request_.usrkey();
        //redis键值对是用户名和用户名+密码+salt组成的字符串生成的16字节的字符串，即usrname和usrkey，这个key其实是value
        
        //std::cout<< "接收到:"<< usr_name <<std::endl;
        //std::thread::id tid = std::this_thread::get_id();
        //std::cout << "当前线程id id=" << tid << std::endl;

        //用户名权限校验
        if (checkUserInfo(usr_name,usr_key,redisconn,mysqlconnection)){
            //用户名权限校验成功：
            //重复性校验
            //判断是否已经抢到过了
            if (!checkUserHasGot(usr_name,usr_key,redisconn,mysqlconnection)){ //还没抢到过
                //开始抢购
                seckillGoods(usr_name,usr_key,redisconn,mysqlconnection,0);
            }
            else{ //该用户已经抢到过
                std::cout << "Seckill failed!!! usr has got" <<std::endl; //用户usr已经抢到了，不能再抢
                response_.set_result("0");
            }
        }
        else{ //用户名权限校验失败：
            std::cout << "Seckill failed!!! usr has no permission" <<std::endl;
            response_.set_result("0");
        }
        redis_->close(redisconn);
        mysql_->close(mysqlconnection);

        responder_.Finish(response_, Status::OK, this);
    } 
    //<3>如果状态是FINISH
    else{
        GPR_ASSERT(status_ == FINISH);
          
        redisContext *redisconn = redis_->getOneConnect();
        redisCommand(redisconn, "lpop %s","work_list");
        redis_->close(redisconn);
          
        delete this;
    }
}

//检查用户信息是否正确
bool Worker::checkUserInfo(std::string usr_name,std::string usr_key,redisContext *redisconn,MYSQL *mysqlcon)
{
  //如果redis连接池中没有redis连接可用了，就只能拿到mysql中去验证用户信息。
  //（为何不多开一点redis连接，肯定不行啊，redis也要有个限度吧！不能开无限多啊。）
  //但是这些redis连接，已经足够减轻mysql数据库的压力了！
  if (redisconn == NULL)
  {
    return checkUserInfo_mysql(usr_name,usr_key,mysqlcon);;
  }
  //哈希表usr_info中，获取usr_name这个value对应的field，即获取usr_key
  redisReply *confirmReply = (redisReply *)redisCommand(redisconn, "HGET usr_info %s", usr_name.c_str()); 
  if (confirmReply != NULL && confirmReply->type == REDIS_REPLY_STRING ) {
      std::string key(confirmReply->str); //key保存从redis缓存中获取到的usr_key
      if (usr_key == key) { //和传进来的usr_key进行比较，如果和redis中的一样，表示用户信息正确
          return true;
      }
  }
  freeReplyObject(confirmReply);
  return false;
}

//到mysql中去验证用户信息
bool Worker::checkUserInfo_mysql(std::string usr_name,std::string usr_key,MYSQL *mysqlcon){
  bool isUsr = false;
  char selSql[200] = {'\0'};
  sprintf(selSql, "SELECT usr_key FROM usr_info WHERE usr_name ='%s'", usr_name.c_str()); //去usr_info表中查询usr_name的usr_key是多少
  std::cout<< selSql <<std::endl;
  if(mysql_query(mysqlcon, selSql)) //sql语句执行失败：
  {
      std::cout << "selSql usr_key was Error:" << mysql_error(mysqlcon);
      return false;
  }
  else //sql语句执行成功：
  {
      MYSQL_RES *result = mysql_use_result(mysqlcon); // 获取结果集
      MYSQL_ROW row;
      while((row = mysql_fetch_row(result)) != NULL) //从结果集中取出一行
      {
          if (mysql_num_fields(result) > 0) //结果集中有几列
          {
              // selUsr_key = std::string(row[0]);
            std::string seusr_key(row[0]);
            if (seusr_key == usr_key) //如果mysq中得到的usr_key，即seusr_key等于传进来的用户的usr_key，说明用户信息正确。校验成功
            {
              isUsr = true;
            }
          }
      }
      if (result != NULL)
      {
        mysql_free_result(result);
      }
  }
  return isUsr;
}


//检查用户是否抢到过商品了。
//原理和检查用户信息是一样的。
bool Worker::checkUserHasGot(std::string usr_name,std::string usr_key,redisContext *redisconn,MYSQL *mysqlcon)
{
  if (redisconn == NULL) //同样，如果redis连接不够了，就去mysql中检查。
  {
    return checkUserHasGot_mysql(usr_name,usr_key,mysqlcon);;
  }
  redisReply *orderReply = (redisReply *)redisCommand(redisconn, "HGET order_info %s", usr_name.c_str()); //HGET，返回哈希表order_info中指定字段的值
  if (orderReply != NULL && orderReply->type == REDIS_REPLY_STRING ) {
      if (orderReply->str != NULL) {
          return true;
      }
  }
  freeReplyObject(orderReply);
  return false;
}

//去mysql中检查用户是否抢到过商品了。
bool Worker::checkUserHasGot_mysql(std::string usr_name,std::string usr_key,MYSQL *mysqlcon)
{
  bool hasgot = false;
  // bool isSuc = true;
  int ON = 1;
  int OFF = 0;
  mysql_autocommit(mysqlcon,OFF);


  char selSql[200] = {'\0'};
  sprintf(selSql, "SELECT * FROM order_info WHERE usr_name ='%s' FOR UPDATE", usr_name.c_str());
  std::string selUsr_key;
  
  if(mysql_query(mysqlcon, selSql))
  {
      std::cout << "selSql usr_key was Error:" << mysql_error(mysqlcon);
      hasgot = false;
  }
  else
  {
      MYSQL_RES *result = mysql_use_result(mysqlcon); // 获取结果集
      MYSQL_ROW row;
      while((row = mysql_fetch_row(result)) != NULL)
      {
          if (mysql_num_fields(result) > 0) 
          {
            hasgot = true;
          }
      }
      if (result != NULL)
      {
        mysql_free_result(result);
      }
  }
  
  // if(!isSuc)
  // {
  //   mysql_rollback(connection);
  //   pthread_rwlock_wrlock(rwlock_);
  //   ++ *failedCount_;
  //   pthread_rwlock_unlock(rwlock_);

  //   response_.set_result("0");
  // }
  // else
  // {
  //   mysql_commit(connection); 
  //   response_.set_result("1"); 
  //   std::cout << "Seckill Succeed!!!" <<std::endl; 
  // }
  mysql_autocommit(mysqlcon,ON);

  return hasgot;
}


//正式抢购商品
//同样，redis连接不够就去mysql中执行。
void Worker::seckillGoods(std::string usr_name,std::string usr_key,redisContext *redisconn,MYSQL *connection,int repeatCount)
{
  if (redisconn == NULL)
  {
    seckillGoods_mysql(usr_name,usr_key,connection);
    return;
  }
  ++repeatCount;
  std::cout << "satrt shopping!" << std::endl;
  int currGoods_count = 0;
  
  //Redis Watch 命令用于监视一个(或多个) key ，如果在事务执行之前这个(或这些) key 被其他命令所改动，那么事务将被打断
  redisReply *watchReply = (redisReply *)redisCommand(redisconn, "WATCH %s", "total_count");

  redisReply *reply = (redisReply *)redisCommand(redisconn, "GET %s", "total_count"); //获取key：total_count对应的value值
  
  if (reply != NULL && watchReply != NULL && watchReply->type == REDIS_REPLY_STATUS && (strcasecmp(watchReply->str,"OK") == 0)) 
  {
      currGoods_count = atoi(reply->str); //当前商品数量
      
      freeReplyObject(watchReply);
      freeReplyObject(reply);
  }else
  {
      freeReplyObject(watchReply);
      freeReplyObject(reply);
      response_.set_result("0");
      return;
  }
  
  if (currGoods_count > 0)  //商品数量要大于0，才可以进行抢购：
  {
      //开启事务
      redisReply *multiReply = NULL;

      //Redis Multi 命令用于标记一个事务块的开始。
      //事务块内的多条命令会按照先后顺序被放进一个队列当中，最后由 EXEC 命令，原子性(atomic)地执行。 
      multiReply  = (redisReply *)redisCommand(redisconn, "MULTI");
      if (multiReply != NULL && multiReply->type == REDIS_REPLY_STATUS && (strcasecmp(multiReply->str,"OK") == 0)) 
      {
          
          //订单操作
          /*Redis Decr 命令将 key 中储存的数字值减一。
            如果 key 不存在，那么 key 的值会先被初始化为 0 ，然后再执行 DECR 操作。
            如果值包含错误的类型，或字符串类型的值不能表示为数字，那么返回一个错误。
            本操作的值限制在 64 位(bit)有符号数字表示之内。
          */
          redisCommand(redisconn, "DECR %s", "total_count"); //商品数量减一

          /*
          Redis Hmset 命令用于同时将多个 field-value (字段-值)对设置到哈希表中。
          此命令会覆盖哈希表中已存在的字段。
          如果哈希表不存在，会创建一个空哈希表，并执行 HMSET 操作。
          */
          redisCommand(redisconn, "HMSET order_info  %s %s", usr_name.c_str(), usr_key.c_str()); //这个用来模拟在mysql中执行插入usr_name和usr_key到order_info
          
          redisReply *execReply = NULL;
          execReply  = (redisReply *)redisCommand(redisconn, "EXEC"); //Redis Exec 命令用于执行所有事务块内的命令。
          if (execReply != NULL && execReply->type == REDIS_REPLY_ARRAY && (execReply->elements > 1)) 
          {
              
              freeReplyObject(execReply);
              seckillGoods_mysql(usr_name,usr_key,connection);
              return;
            }else 
            {
              //失败重试
              std::cout << "retry!!!" <<std::endl;
              if (execReply != NULL ) 
              {
                  freeReplyObject(execReply);
              }
              redisCommand(redisconn, "UNWATCH"); //Redis Unwatch 命令用于取消 WATCH 命令对所有 key 的监视。
              if (repeatCount < RETRYCOUNT) //如果seckill抢购商品失败，总共可以执行RETRYCOUNT次。如果重复次数小于阈值，就失败重试
              {
                  seckillGoods(usr_name,usr_key,redisconn,connection,repeatCount);
              }else //重复次数过多了，直接返回0，表示抢购失败。
              {
                  response_.set_result("0");
              }
            }
          
      }else 
      {
          redisCommand(redisconn, "UNWATCH");
          if (repeatCount < RETRYCOUNT) 
          {
              seckillGoods(usr_name,usr_key,redisconn,connection,repeatCount);
          }else
          {
              response_.set_result("0");
          }
      }
      freeReplyObject(multiReply);
      return;
  }else  //如果商品数量小于等于0，没有了，无法进行抢购。
  {
      redisCommand(redisconn, "UNWATCH");

      pthread_rwlock_wrlock(rwlock_); //读写锁上锁
      if (*failedCount_ > 0) //要将失败的抢购个数，返回到商品总数中。
      {
        
        std::string failedC = std::to_string(*failedCount_);
        redisReply *setCreply = (redisReply *)redisCommand(redisconn, "SET total_count %s", failedC.c_str());
        if (setCreply != NULL &&  setCreply->type == REDIS_REPLY_STATUS && (strcasecmp(setCreply->str,"OK") == 0))
        {
          *failedCount_ = 0;
        }

      }
      pthread_rwlock_unlock(rwlock_); //读写锁解锁
      
      //但是本次抢购操作依然是失败
      response_.set_result("0");
      std::cout << "seckill failed! The goods has sold out!" << std::endl;
      return;
  }
}

void Worker::seckillGoods_mysql(std::string usr_name,std::string usr_key,MYSQL *connection)
{
        bool isSuc = true;
        int ON = 1;
        int OFF = 0;
        mysql_autocommit(connection,OFF);
        

        int total_count = 0;
        std::string selSql("SELECT g_totalcount FROM goods_info WHERE g_id ='10' FOR UPDATE");
        if(mysql_query(connection, selSql.c_str())) //sql语句执行失败：
        {
            std::cout << "selSql was Error:" << mysql_error(connection);
            isSuc = false;
        }
        else //sql语句执行成功：
        {
            MYSQL_RES *result = mysql_use_result(connection); // 获取结果集
            MYSQL_ROW row;
            while((row = mysql_fetch_row(result)) != NULL)
            {
                if (mysql_num_fields(result) > 0) 
                {
                    total_count = atoi(row[0]); //先获取当前商品数量
                }
            }
            if (result != NULL)
            {
              mysql_free_result(result);
            }
        }

        if (total_count > 0)
        {
          -- total_count;
        }else //如果当前商品数量小于等于0，无法进行抢购服务
        {
          // mysql_rollback(connection);
          response_.set_result("0"); //返回0，表示失败
          mysql_autocommit(connection,ON);
          return;
        }

        char updateSql[200] = {'\0'}; //描述一种c字符串的格式。
        sprintf(updateSql, "UPDATE goods_info SET g_totalcount = %d WHERE g_id = '10' ", total_count); //更新商品数量。将这个按照上述c字符串格式转换，下面要用
        if(mysql_query(connection,updateSql)) //sql语句执行失败：
        {
          std::cout << "updateSql was error:"<< updateSql << std::endl;
          isSuc = false;
        }

        char insertOSql[200] = {'\0'};
        //抢购成功，完成一笔订单，插入到order_info表中
        sprintf(insertOSql, "INSERT INTO order_info(usr_name,usr_key,goods_id) VALUES ('%s','%s','10')", usr_name.c_str(),usr_key.c_str());
        // std::cout << insertOSql<< std::endl;
        if(mysql_query(connection,insertOSql))
        {
          std::cout << "insertOSql was error" << std::endl;
          isSuc = false;
        }


        if(!isSuc) //如果没有seckill成功：
        {
          mysql_rollback(connection); //mysql事务回退
          pthread_rwlock_wrlock(rwlock_);
          ++ *failedCount_; //失败次数加一
          pthread_rwlock_unlock(rwlock_);

          response_.set_result("0");
        }
        else //seckill成功：
        {
          mysql_commit(connection);  //mysql事务提交
          response_.set_result("1");
            ++ *successNum_; //成功次数加一
          std::cout << "Seckill Succeed!!!" <<std::endl; 
        }
        mysql_autocommit(connection,ON);
}


//Worker类封装了异步请求处理所需要的状态与逻辑
//客户端异步调用seckill请求服务。（异步调用，但是阻塞等待从cq中取回结果。）
//传进来的usr_name和usr_key。
//我们先用redis来进行相关的校验。包括：校验用户信息，校验用户是否抢到过商品。
//这些可以只用redis操作，只有redis连接池中的redis连接不够了，采用mysql连接，直接在数据库中操作。
//然后校验完了，正式seckill抢购的时候。
//也是先用redis，开启一个事务：商品数量减一，用一个哈希表usr_info中，插入usr_name和usr_key来模拟mysql中对usr_info表的插入。
//如果redis事务处理成功了，表示可以进行，这时候就到mysql中执行相关事务，包括：更新goods_info表中的商品数量，插入usr_info,usr_key,"10"到order_info订单表。
//同样，如果redis连接不够了，直接用mysql进行操作。
