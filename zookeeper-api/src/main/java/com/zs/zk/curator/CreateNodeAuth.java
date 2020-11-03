package com.zs.zk.curator;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryUntilElapsed;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Id;
import org.apache.zookeeper.server.auth.DigestAuthenticationProvider;

import java.util.ArrayList;

public class CreateNodeAuth {

    public static void main(String[] args) throws Exception {

        RetryPolicy retryPolicy = new RetryUntilElapsed(5000, 1000);

        CuratorFramework client = CuratorFrameworkFactory
                .builder()
                .connectString("192.168.1.105:2181")
                .sessionTimeoutMs(5000)
                .connectionTimeoutMs(5000)
                .retryPolicy(retryPolicy)
                .build();

        client.start();

        ACL aclIp = new ACL(ZooDefs.Perms.READ, new Id("ip", "192.168.1.105"));
        ACL aclDigest = new ACL(ZooDefs.Perms.READ | ZooDefs.Perms.WRITE, new Id("digest", DigestAuthenticationProvider.generateDigest("jike:123456")));
        ArrayList<ACL> acls = new ArrayList<ACL>();
        acls.add(aclDigest);
        acls.add(aclIp);

        String path = client.create()
                .creatingParentsIfNeeded()
                .withMode(CreateMode.PERSISTENT)
                .withACL(acls)
                .forPath("/jike/3", "123".getBytes());

        System.out.println(path);

        Thread.sleep(Integer.MAX_VALUE);


    }

}