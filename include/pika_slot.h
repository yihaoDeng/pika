#ifndef PIKA_SLOT_H_
#define PIKA_SLOT_H_

#include "pink/include/pink_thread.h"
#include "pink/include/pink_cli.h"
#include "include/pika_command.h"
#include "include/pika_client_conn.h"
#include "strings.h"

const std::string SlotKeyPrefix = "_internal:slotkey:4migrate:";
const size_t MaxKeySendSize = 10 * 1024;
//crc 32
#define HASH_SLOTS_MASK 0x000003ff
#define HASH_SLOTS_SIZE (HASH_SLOTS_MASK + 1)

const uint32_t IEEE_POLY = 0xedb88320;
extern uint32_t crc32tab[256];

void CRC32TableInit(uint32_t poly);

extern void InitCRC32Table();

extern uint32_t CRC32Update(uint32_t crc, const char *buf, int len);

extern int SlotNum(const std::string &str);

extern void SlotKeyAdd(const std::string type, const std::string key);

#endif
