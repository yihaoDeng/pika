#include "include/pika_slaveof_redis_thread.h"

namespace monica {

using namespace pink;


PikaRedisMasterConn::PikaRedisMasterConn(PinkEpoll* pink_epoll, void *specific_data)
      : PinkConn(-1, "", NULL, pink_epoll), 
        redis_cli_(NewRedisCli()), state_(kReplNone) {
      }

ReadStatus PikaRedisMasterConn::GetRequest() {
  return kReadError;   
}
WriteStatus PikaRedisMasterConn::SendReply() {
  if (state_ == kReplNone) {
    return kWriteError;
  } 
  if (state_ == kReplConnecting) {
    pink_epoll()->PinkModEvent(redis_cli_->fd(), 0, EPOLLIN); 
    state_ = kReplRecvPong;
  } 
  return kWriteAll;
}

bool PikaRedisMasterConn::SetRedisMaster(const std::string& ip, int port) {
  slash::Status s = redis_cli_->Connect(ip, port); 
  if (s.ok()) {
    state_ = kReplConnecting;
    return true;
  } 
  set_fd(redis_cli_->fd());    
  //SetNonblock(); 
  //pink_epoll()->PinkModEvent(redis_cli_->fd(), 0, EPOLLOUT | EPOLLIN); 
  return false;
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
              pink_epoll_->PinkAddEvent(ti.fd(), EPOLLIN | EPOLLOUT);
              //if (ti.notify_type() == kNotiConnect) {
              //  std::shared_ptr<PinkConn> tc = conn_factory_->NewPinkConn(
              //      ti.fd(), ti.ip_port(),
              //      server_thread_, private_data_, pink_epoll_);
              //  if (!tc || !tc->SetNonblock()) {
              //    continue;
              //  }

              //  {
              //    slash::WriteLock l(&rwlock_);
              //    conns_[ti.fd()] = tc;
              //  }
              //} else if (ti.notify_type() == kNotiClose) {
              //  // should close?
              //} else if (ti.notify_type() == kNotiEpollout) {
              //  pink_epoll_->PinkModEvent(ti.fd(), 0, EPOLLOUT);
              //} else if (ti.notify_type() == kNotiEpollin) {
              //  pink_epoll_->PinkModEvent(ti.fd(), 0, EPOLLIN);
              //} else if (ti.notify_type() == kNotiEpolloutAndEpollin) {
              //  pink_epoll_->PinkModEvent(ti.fd(), 0, EPOLLOUT | EPOLLIN);
              //}
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
          in_conn->set_last_interaction(now);
          if (read_status == kReadAll) {
            pink_epoll_->PinkModEvent(pfe->fd, 0, 0);
            // Wait for the conn complete asynchronous task and
            // Mod Event to EPOLLOUT
          } else if (read_status == kReadHalf) {
            continue;
          } else {
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
void PikaSlaveofRedisThread::Cleanup() { 
  slash::WriteLock l(&rwlock_);
  for (auto& iter : conns_) {
    close(iter.first);
  }
  conns_.clear();
}

} // namespace monica
