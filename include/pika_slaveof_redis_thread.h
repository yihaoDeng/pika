#ifndef PIKA_SLAVEOF_REDIS_H_ 
#define PIKA_SLAVEOF_REDIS_H_
#include <unistd.h>
#include <map>
#include <queue>
#include "pink/include/pink_thread.h"
#include "pink/include/pink_conn.h" 
#include "pink/include/pink_cli.h"
#include "pink/include/redis_cli.h"
#include "pink/include/pink_thread.h"
#include "slash/include/slash_mutex.h"
#include "include/pika_client_conn.h"
#include "include/pika_define.h"
#include "include/pika_conf.h"
#include "include/pika_server.h"
#include "slash/include/slash_status.h"
#include "slash/include/slash_slice.h"
#include "slash/include/env.h"

extern PikaServer* g_pika_server;
extern PikaConf* g_pika_conf;

namespace monica {

using namespace pink;

const size_t kReplSyncioTimeout = 10 * 1000; // repl read、write timeout
const size_t kConfigRunIdSize = 40;

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

enum PsyncState {
  kPsyncWriteError = 0, 
  kPsyncWaitReply,
  kPsyncContinue,
  kPsyncFullResync,
  kPsyncNotSupported,
  kPsyncTryLater 
};

struct ReplInfo {
  uint64_t read_reploff;  
  uint64_t reploff;
  uint64_t repl_ack_off;
  uint64_t repl_ack_time;
  uint64_t repl_init_offset;
  uint64_t repl_transfer_size;
  uint64_t repl_transfer_read;
  char run_id[kConfigRunIdSize + 1];
};

class PikaSlaveofRedisThread;

class PikaSyncRedisConn : public PinkConn {
  public:   
    PikaSyncRedisConn(PinkEpoll* pink_epoll, void *worker_specific_data, PikaSlaveofRedisThread *thread);

    virtual ~PikaSyncRedisConn();
    virtual ReadStatus GetRequest();   
    virtual WriteStatus SendReply();

    bool SetRedisAsMaster(const std::string& ip, int port); 

    PsyncState TryPartialResync(bool read_reply);  
  private:
    PinkCli* redis_cli_;
    PinkConn* redis_conn_;
    PikaSlaveofRedisThread *thread_; 
    std::string master_ip_;
    int master_port_;
    //char run_id[40 + 1];  

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
      PikaSyncRedisConn *conn = new PikaSyncRedisConn(pink_epoll_, NULL, this);  
      if (!conn->SetRedisAsMaster(ip, port)) {
        return false;
      } 
      pink_epoll_->notify_queue_lock(); 
      conn_queue_.emplace(conn);
      pink_epoll_->notify_queue_unlock();
      write(pink_epoll_->notify_send_fd(), "", 1);
      return true;
    }
    void ResetRepl() {
      delete rdb_file_;
      repl_state_ = kReplConnect;
    }
    void Cleanup();
    void DoCronTask() {
      //nothing to do
    }

    ReplState repl_state() const {
      return repl_state_;
    } 
    void set_repl_state(ReplState state) {
      repl_state_ = state; 
    }

    PsyncState psync_state() const {
      return psync_state_;
    }
    void set_psync_state(PsyncState state) {
      psync_state_ = state; 
    } 
    slash::Status CreateRdbFile();
    
    ReplInfo *cached_master() {
      return cached_master_;
    }
    void ClearCachedMaster() {
      delete cached_master_; 
      cached_master_ = nullptr;
    }
     
  private:  
    virtual void* ThreadMain();
    // epoll handler
    pink::PinkEpoll *pink_epoll_;
    int cron_interval_;

    ReplState repl_state_;

    slash::WritableFile *rdb_file_;
    std::string master_ip_;
    int master_port_;
    PsyncState psync_state_;

    int64_t master_init_offset; 
    ReplInfo curr;
    ReplInfo *cached_master_;

    mutable slash::RWMutex rwlock_;
    std::queue<std::shared_ptr<PinkConn>> conn_queue_; 
    std::map<int, std::shared_ptr<PinkConn>> conns_;

    //No copy allowed
    PikaSlaveofRedisThread(const PikaSlaveofRedisThread&);
    void operator=(const PikaSlaveofRedisThread&); 
};  

} // namespace monica
#endif
