#ifndef PIKA_SLAVEOF_REDIS_H_ 
#define PIKA_SLAVEOF_REDIS_H_
#include <unistd.h>
#include <map>
#include <queue>
#include "pink/include/pink_thread.h"
#include "pink/include/pink_conn.h" 
#include "pink/include/pink_cli.h"
#include "pink/include/pink_thread.h"
#include "slash/include/slash_mutex.h"
#include "include/pika_client_conn.h"
#include "include/pika_define.h"
#include "slash/include/slash_status.h"
#include "slash/include/slash_slice.h"

namespace monica {

using namespace pink;

const size_t kReplSyncioTimeout = 10;

enum ReplState {
  kReplNone = 0,
  kReplConnect,
  kReplConnecting,
  kReplRecvPong,
  kReplSendAuth,  
  kReplRecvAuth,  
  kReplSendPort,
  kReplRecvPort,
  kReplSendIp,
  kReplRecvIp,
  kReplSendCapa,
  kReplRecvCapa,
  kReplSendPsync, 
  kReplRecvPsync,
  kReplTransfer, 
  kReplConnected
};

class PikaRedisMasterConn : public PinkConn {
  public:   
    PikaRedisMasterConn(PinkEpoll* pink_epoll, void *worker_specific_data);

    virtual ~PikaRedisMasterConn() {
      delete redis_cli_; 
      delete redis_conn_;
    } 
    virtual ReadStatus GetRequest();   
    virtual WriteStatus SendReply();

    bool SetRedisMaster(const std::string& ip, int port); 
    ReplState repl_state() {
      return state_;
    } 
  private:
    PinkCli* redis_cli_;
    ReplState state_;
    PinkConn* redis_conn_;
    char run_id[40 + 1];  
};

class PikaSlaveofRedisThread : public Thread {
  public:
    PikaSlaveofRedisThread(int cron_interval = 1000)
     : pink_epoll_(new PinkEpoll()), cron_interval_(cron_interval) {
    }
    virtual ~PikaSlaveofRedisThread() {
      delete pink_epoll_; 
    }
    bool SetMaster(const std::string &ip, int port) {
      PikaRedisMasterConn *conn = new PikaRedisMasterConn(pink_epoll_, NULL);  
      if (!conn->SetRedisMaster(ip, port)) {
        return false;
      } 
      pink_epoll_->notify_queue_lock(); 
      conn_queue_.emplace(conn);
      pink_epoll_->notify_queue_unlock();
      write(pink_epoll_->notify_send_fd(), "", 1);
      return true;
    }
    void Cleanup();
    void DoCronTask() {
      //nothing to do
    }
  private:  
    virtual void* ThreadMain();
    // epoll handler
    pink::PinkEpoll *pink_epoll_;

    int cron_interval_;
    //No copy allowed
    mutable slash::RWMutex rwlock_;
    std::queue<std::shared_ptr<PinkConn>> conn_queue_; 
    std::map<int, std::shared_ptr<PinkConn>> conns_;

    PikaSlaveofRedisThread(const PikaSlaveofRedisThread&);
    void operator=(const PikaSlaveofRedisThread&); 
};  

} // namespace monica
#endif
