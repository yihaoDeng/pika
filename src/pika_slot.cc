// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include <algorithm>
#include "slash/include/slash_string.h"
#include "slash/include/slash_status.h"
#include "include/pika_conf.h"
#include "include/pika_slot.h"
#include "include/pika_server.h"

#define min(a, b)  (((a) > (b)) ? (b) : (a))

extern PikaServer *g_pika_server;
extern PikaConf *g_pika_conf;

const int asyncRecvsNum = 64;
uint32_t crc32tab[256];
void CRC32TableInit(uint32_t poly) {
    int i, j;
    for (i = 0; i < 256; i ++) {
        uint32_t crc = i;
        for (j = 0; j < 8; j ++) {
            if (crc & 1) {
                crc = (crc >> 1) ^ poly;
            } else {
                crc = (crc >> 1);
            }
        }
        crc32tab[i] = crc;
    }
}

void InitCRC32Table() {
    CRC32TableInit(IEEE_POLY);
}

uint32_t CRC32Update(uint32_t crc, const char *buf, int len) {
    int i;
    crc = ~crc;
    for (i = 0; i < len; i ++) {
        crc = crc32tab[(uint8_t)((char)crc ^ buf[i])] ^ (crc >> 8);
    }
    return ~crc;
}

// get key slot number
int SlotNum(const std::string &str) {
    uint32_t crc = CRC32Update(0, str.data(), (int)str.size());
    return (int)(crc & HASH_SLOTS_MASK);
}

// add key to slotkey
void SlotKeyAdd(const std::string type, const std::string &key) {
    if (g_pika_conf->slotmigrate() != true){
        return;
    }
    int32_t res = 0;
    std::string slotKey = SlotKeyPrefix + std::to_string(SlotNum(key)); 
    std::vector<std::string> members = {type + key}; 
    rocksdb::Status s = g_pika_server->db()->SAdd(slotKey, {type+key}, &res);
    if (!s.ok()) {
        LOG(WARNING) << "Zadd key: " << key <<" to slotKey, error: " <<strerror(errno);
    }
}







