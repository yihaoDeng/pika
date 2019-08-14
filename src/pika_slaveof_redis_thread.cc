#include "include/pika_slaveof_redis_thread.h"

extern PikaServer* g_pika_server;
extern PikaConf* g_pika_conf;

namespace monica {

using namespace pink;
using namespace slash;


PikaSyncRedisConn::PikaSyncRedisConn(
    PinkEpoll* pink_epoll, void *specific_data,
    PikaSlaveofRedisThread *thread)
  : PinkConn(-1, "", NULL, pink_epoll), 
  redis_cli_(NewRedisCli()), thread_(thread) {
  }
PikaSyncRedisConn::~PikaSyncRedisConn() {
  thread_->ResetRepl();
  delete redis_cli_; 
  delete redis_conn_;
}
ReadStatus PikaSyncRedisConn::GetRequest() {
  pink::RedisCmdArgsType argv;
  slash::Status s;
  ReplState state = thread_->repl_state();
  if (state == kReplRecvPong) {
    s = redis_cli_->Recv(&argv); 
    if (!s.ok() && argv[0] != "NOAUTH" && argv[0] != "ERR operation not permitted") {
      return kReadError;
    } else {
      state = kReplSendAuth; 
    }
  } else if (state == kReplRecvAuth) {
    s = redis_cli_->Recv(&argv); 
    if (!s.ok() || argv[0] != "OK") {
      return kReadError; 
    }
    state = kReplSendPort;
  } else if (state == kReplRecvPort) {
    s = redis_cli_->Recv(&argv);
    if (!s.ok()) {
      LOG(WARNING) << "Recv send-port response err:" << s.ToString();
      return kReadError; 
    }
    if (argv[0] != "OK") {
      LOG(WARNING) << "(Non critical) Master does not understand "
        << "REPLCONF listening-port:" << argv[0];
    } 
    state = kReplSendIp;
  } else if (state == kReplRecvIp) {
    s = redis_cli_->Recv(&argv); 
    if (!s.ok()) {
      LOG(WARNING) << "Recv send-ip response err:" << s.ToString();
      return kReadError; 
    } 
    if (argv[0] != "OK") {
      LOG(WARNING) << "(Non critical) Master does not understand "
        << "REPLCONF ip-address" << argv[0];
    } 
    state = kReplSendCapa;
  } else if (state == kReplRecvCapa) {
    s = redis_cli_->Recv(&argv); 
    if (!s.ok()) {
      LOG(WARNING) << "Recv REPLCONF capa  response err:" << s.ToString();
      return kReadError; 
    }
    state = kReplSendPsync; 
  }
  thread_->set_repl_state(state);
  set_is_reply(true);
  return kReadAll;   
}

WriteStatus PikaSyncRedisConn::SendReply() {
  pink::RedisCmdArgsType argv;
  std::string wbuf_str;
  slash::Status s;

  ReplState state = thread_->repl_state();
  if (state == kReplNone) {
    return kWriteError;
  } else if (state == kReplConnecting) {
    pink_epoll()->PinkModEvent(redis_cli_->fd(), 0, EPOLLIN); 
    //setup timeout and sync write, 
    argv.push_back("PING");
    SerializeRedisCommand(argv, &wbuf_str);
    s = redis_cli_->Send(&wbuf_str); 
    if (!s.ok()) {
      return kWriteError;
    }
    thread_->set_repl_state(kReplRecvPong);
    return kWriteAll;
  } else if (state == kReplSendAuth) {
    if (g_pika_conf->masterauth().empty()) {
      state = kReplSendPort;
    } else {
      argv.push_back("AUTH"); 
      argv.push_back(g_pika_conf->masterauth()); 
      SerializeRedisCommand(argv, &wbuf_str);
      s = redis_cli_->Send(&wbuf_str);
      if (!s.ok()) {
        return kWriteError;
      }
      thread_->set_repl_state(kReplRecvAuth);
      return kWriteAll;
    }
  }

  if (state == kReplSendPort) {
    argv.push_back("REPLCONF"); 
    argv.push_back("listening-port");
    argv.push_back(std::to_string(g_pika_conf->port()));
    SerializeRedisCommand(argv, &wbuf_str);
    s = redis_cli_->Send(&wbuf_str);
    if (!s.ok()) {
      return kWriteError; 
    } 
    thread_->set_repl_state(kReplRecvPort);
    return kWriteAll;
  } else if (state == kReplSendIp) {
    std::string host = g_pika_server->host();
    argv.push_back("REPLCONf");  
    argv.push_back("ip-address");
    argv.push_back(host);
    SerializeRedisCommand(argv, &wbuf_str);
    s = redis_cli_->Send(&wbuf_str);
    if (!s.ok()) {
      return kWriteError;
    }
    thread_->set_repl_state(kReplRecvIp);
    return kWriteAll;
  } else if (state == kReplSendCapa) {
    argv.push_back("REPLCONF");
    argv.push_back("capa");
    argv.push_back("eof");
    argv.push_back("capa");
    argv.push_back("psync2");
    SerializeRedisCommand(argv, &wbuf_str);
    s = redis_cli_->Send(&wbuf_str);
    if (!s.ok()) {
      return kWriteError;
    }
    thread_->set_repl_state(kReplRecvCapa);
    return kWriteAll;
  } else if (state == kReplSendPsync) {
    thread_->set_psync_state(TryPartialResync(false));  
    if (thread_->psync_state() == kPsyncWriteError) {
      return kWriteError;
    }
    thread_->set_repl_state(kReplRecvPsync);
    return kWriteAll;
  }

  if (state != kReplRecvPsync) {
    return kWriteError;
  }

  thread_->set_psync_state(TryPartialResync(true));
  PsyncState psync_state = thread_->psync_state();
  if (psync_state == kPsyncWaitReply || psync_state == kPsyncContinue) {
    return kWriteAll;
  } else if (psync_state == kPsyncTryLater) {
    return kWriteError;
  } else if (psync_state == kPsyncNotSupported) {
    argv.clear();  
    argv.push_back("SYNC");
    SerializeRedisCommand(argv, &wbuf_str); 
    s = redis_cli_->Send(&wbuf_str);
    if (!s.ok()) {
      return kWriteError;
    }
  }  
  s = thread_->CreateRdbFile();
  if (!s.ok()) {
    return kWriteError;
  }
  return kWriteAll;
}

PsyncState PikaSyncRedisConn::TryPartialResync(bool read_reply) {
  return kPsyncNotSupported;    
}
bool PikaSyncRedisConn::SetRedisAsMaster(const std::string& ip, int port) {
  slash::Status s = redis_cli_->Connect(ip, port); 
  if (!s.ok()) {
    return false;
  } 
  master_ip_ = ip;
  master_port_ = port;
  set_fd(redis_cli_->fd());    
  redis_cli_->set_recv_timeout(kReplSyncioTimeout);
  redis_cli_->set_send_timeout(kReplSyncioTimeout);
  return true;
}

void *PikaSlaveofRedisThread::ThreadMain() {
  int nfds;
  PinkFiredEvent *pfe = NULL;
  char bb[2048];
  std::shared_ptr<PinkConn> in_conn = nullptr;

  struct timeval when;
  gettimeofday(&when, NULL);
  struct timeval now = when;

  when.tv_sec += (cron_interval_ / 1000);
  when.tv_usec += ((cron_interval_ % 1000) * 1000);
  int timeout = cron_interval_;
  if (timeout <= 0) {
    timeout = PINK_CRON_INTERVAL;
  }
  while (!should_stop()) {
    if (cron_interval_ > 0) {
      gettimeofday(&now, NULL);
      if (when.tv_sec > now.tv_sec ||
          (when.tv_sec == now.tv_sec && when.tv_usec > now.tv_usec)) {
        timeout = (when.tv_sec - now.tv_sec) * 1000 +
          (when.tv_usec - now.tv_usec) / 1000;

      } else {
        DoCronTask();
        when.tv_sec = now.tv_sec + (cron_interval_ / 1000);
        when.tv_usec = now.tv_usec + ((cron_interval_ % 1000) * 1000);
        timeout = cron_interval_;
      }
    }
    nfds = pink_epoll_->PinkPoll(timeout);
    for (int i = 0; i < nfds; i++) {
      pfe = (pink_epoll_->firedevent()) + i;
      if (pfe->fd == pink_epoll_->notify_receive_fd()) {
        if (pfe->mask & EPOLLIN) {
          int32_t nread = read(pink_epoll_->notify_receive_fd(), bb, 2048);
          if (nread == 0) {
            continue;
          } else {
            for (int32_t idx = 0; idx < nread; ++idx) {
              std::shared_ptr<PinkConn> conn;
              {
                pink_epoll_->notify_queue_lock();
                conn = conn_queue_.front(); 
                conn_queue_.pop();
                pink_epoll_->notify_queue_unlock();
              }

              {
                slash::WriteLock l(&rwlock_);
                conns_[conn->fd()] = conn;
              }
              if (!conn || !conn->SetNonblock()) {
                continue;
              }   
              // read or write event 
              pink_epoll_->PinkAddEvent(conn->fd(), EPOLLIN | EPOLLOUT);
            }
          }
        } else {
          continue;
        }
      } else {
        in_conn = NULL;
        int should_close = 0;
        if (pfe == NULL) {
          continue;
        }
        std::map<int, std::shared_ptr<PinkConn>>::iterator iter = conns_.find(pfe->fd);
        if (iter == conns_.end()) {
          pink_epoll_->PinkDelEvent(pfe->fd);
          continue;
        }

        in_conn = iter->second;

        if ((pfe->mask & EPOLLOUT) && in_conn->is_reply()) {
          WriteStatus write_status = in_conn->SendReply();
          in_conn->set_last_interaction(now);
          if (write_status == kWriteAll) {
            pink_epoll_->PinkModEvent(pfe->fd, 0, EPOLLIN);
            in_conn->set_is_reply(false);
          } else if (write_status == kWriteHalf) {
            continue;
          } else {
            should_close = 1;
          }
        }

        if (!should_close && (pfe->mask & EPOLLIN)) {
          ReadStatus read_status = in_conn->GetRequest();
          if (read_status != kReadAll || read_status != kReadHalf) {
            should_close = 1;
          } else if (in_conn->is_reply()) {
            WriteStatus write_status = in_conn->SendReply();
            if (write_status == kWriteAll) {
              in_conn->set_is_reply(false);
            } else if (write_status == kWriteHalf) {
              pink_epoll_->PinkModEvent(pfe->fd, EPOLLIN, EPOLLOUT);
            } else if (write_status == kWriteError) {
              should_close = 1;
            }
          } else {
            continue;
          }
        }
        if (pfe->mask & EPOLLOUT) {
          WriteStatus write_status = in_conn->SendReply();
          in_conn->set_last_interaction(now);
          if (write_status == kWriteAll) {
            in_conn->set_is_reply(false);
            pink_epoll_->PinkModEvent(pfe->fd, 0, EPOLLIN);
          } else if (write_status == kWriteHalf) {
            continue;
          } else if (write_status == kWriteError) {
            should_close = 1;
          }

        }

        if ((pfe->mask & EPOLLERR) || (pfe->mask & EPOLLHUP) || should_close) {
          {
            slash::WriteLock l(&rwlock_);
            pink_epoll_->PinkDelEvent(pfe->fd);
            in_conn = NULL;
            conns_.erase(pfe->fd);
            close(in_conn->fd());
          }
        }
      }  // connection event
    }  // for (int i = 0; i < nfds; i++)
  }  // while (!should_stop())

  Cleanup();
  return NULL;
}

slash::Status PikaSlaveofRedisThread::CreateRdbFile() {
  char buf[256]; 
  snprintf(buf, 256, "temp-%ld.%s.%d.rdb", slash::NowMicros(), master_ip_.c_str(), master_port_);
  return slash::NewWritableFile(g_pika_conf->bgsave_path() + buf, &rdb_file_);
}
void PikaSlaveofRedisThread::Cleanup() { 
  slash::WriteLock l(&rwlock_);
  for (auto& iter : conns_) {
    close(iter.first);
  }
  conns_.clear();
}

} // namespace monica
