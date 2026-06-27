/* Copyright (c) 2023 Renmin University of China
RMDB is licensed under Mulan PSL v2. ... */

#pragma once

#include <mutex>
#include <condition_variable>
#include <unordered_map>
#include <list>
#include "transaction/transaction.h"

static const std::string GroupLockModeStr[10] = {"NON_LOCK", "IS", "IX", "S", "X", "SIX"};

/**
 * @brief 多粒度锁 + wait-die 死锁预防。
 *
 * 表级锁支持 IS / IX / S / X / SIX，行级锁支持 S / X。严格 2PL 仍由
 * TransactionManager 在 commit/abort 时统一释放。
 */
class LockManager {
public:
    enum class LockMode { SHARED, EXLUCSIVE, INTENTION_SHARED, INTENTION_EXCLUSIVE, S_IX };
    enum class GroupLockMode { NON_LOCK, IS, IX, S, X, SIX };

    class LockRequest {
    public:
        LockRequest(txn_id_t txn_id, timestamp_t ts, LockMode lock_mode)
            : txn_id_(txn_id), start_ts_(ts), lock_mode_(lock_mode), granted_(false) {}
        txn_id_t txn_id_;
        timestamp_t start_ts_;       // 用于 wait-die
        LockMode lock_mode_;
        bool granted_;
    };

    class LockRequestQueue {
    public:
        std::list<LockRequest> request_queue_;
        std::condition_variable cv_;
        txn_id_t upgrading_txn_ = INVALID_TXN_ID;
        LockMode upgrading_mode_ = LockMode::SHARED;
        // 当前已授予的锁的组模式：NON_LOCK / IS / IX / S / X / SIX
        GroupLockMode group_lock_mode_ = GroupLockMode::NON_LOCK;
    };

public:
    LockManager() {}
    ~LockManager() {}

    // ===== 表级锁 =====
    bool lock_shared_on_table(Transaction* txn, int tab_fd);
    bool lock_exclusive_on_table(Transaction* txn, int tab_fd);

    // ===== 行级锁、意向锁 =====
    bool lock_shared_on_record(Transaction* txn, const Rid& rid, int tab_fd);
    bool lock_exclusive_on_record(Transaction* txn, const Rid& rid, int tab_fd);
    bool lock_IS_on_table(Transaction* txn, int tab_fd);
    bool lock_IX_on_table(Transaction* txn, int tab_fd);

    // ===== 释放：被 TransactionManager 在 commit/abort 末尾调用 =====
    bool unlock(Transaction* txn, LockDataId lock_data_id);
    void unlock_all(Transaction* txn);

    // ===== 兼容旧接口：现已 no-op，事务并发由 lock_table_ 管 =====
    void global_lock()   { /* no-op */ }
    void global_unlock() { /* no-op */ }

private:
    bool lock(Transaction* txn, const LockDataId& lock_data_id, LockMode mode);
    bool lock_table(Transaction* txn, int tab_fd, LockMode mode);

    bool is_compatible(LockMode granted, LockMode requested) const;
    bool is_compatible_with_granted(const LockRequestQueue& queue, txn_id_t txn_id, LockMode requested) const;
    bool should_abort_by_wait_die(const LockRequestQueue& queue, Transaction* txn, LockMode requested) const;
    bool should_abort_by_wait_die(const LockRequestQueue& queue, Transaction* txn, LockMode requested,
                                  std::list<LockRequest>::const_iterator request) const;
    bool can_grant_request(const LockRequestQueue& queue, std::list<LockRequest>::const_iterator request) const;
    bool can_grant_upgrade(const LockRequestQueue& queue, txn_id_t txn_id, LockMode requested) const;
    LockMode upgrade_mode(LockMode held, LockMode requested) const;
    GroupLockMode recompute_group_lock_mode(const LockRequestQueue& queue) const;

    std::mutex latch_;                                          // 保护 lock_table_ 自身
    std::unordered_map<LockDataId, LockRequestQueue> lock_table_;
};
