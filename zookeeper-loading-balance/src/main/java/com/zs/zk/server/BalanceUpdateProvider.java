package com.zs.zk.server;

public interface BalanceUpdateProvider {

    // 增加负载
    public boolean addBalance(Integer step);

    // 减少负载
    public boolean reduceBalance(Integer step);

}