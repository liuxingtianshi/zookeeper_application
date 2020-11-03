package com.zs.zk;

import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.exception.ZkNodeExistsException;
import org.I0Itec.zkclient.serialize.BytesPushThroughSerializer;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * 采用Zookeeper实现分布式ID
 */
public class IdMaker {
    private ZkClient client = null;
    //zk服务器的地址
    private final String server;
    // zookeeper顺序节点的父节点
    private final String root;
    // 顺序节点的名称
    private final String nodeName;
    // 标识当前服务是否正在运行
    private volatile boolean running = false;
    private ExecutorService cleanExecutor = null;

    //删除节点的级别
    public enum RemoveMethod {
        NONE, IMMEDIATELY, DELAY
    }

    public IdMaker(String zkServer, String root, String nodeName) {
        this.root = root;
        this.server = zkServer;
        this.nodeName = nodeName;
    }

    // 启动zk客户端服务
    public void start() throws Exception {
        if (running)
            throw new Exception("zk client has started...");
        running = true;

        init();
    }


    public void stop() throws Exception {
        if (!running)
            throw new Exception("zk client has stopped...");
        running = false;

        freeResource();
    }


    private void init() {
        client = new ZkClient(server, 5000, 5000, new BytesPushThroughSerializer());
        cleanExecutor = Executors.newFixedThreadPool(10);
        try {
            client.createPersistent(root, true);
        } catch (ZkNodeExistsException e) {
            //ignore;
            System.out.println("节点" + this.root + "已经存在。。。");
        }
    }

    // 释放ZK客户端服务资源
    private void freeResource() {

        cleanExecutor.shutdown();
        try {
            cleanExecutor.awaitTermination(2, TimeUnit.SECONDS);

        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally {
            cleanExecutor = null;
        }

        if (client != null) {
            client.close();
            client = null;
        }
    }


    // 检查当前服务是否运行
    private void checkRunning() throws Exception {
        if (!running)
            throw new Exception("请先调用start");
    }

    // 从顺序节点名中提取我们要的ID值
    private String ExtractId(String str) {
        int index = str.lastIndexOf(nodeName);
        if (index >= 0) {
            index += nodeName.length();
            return index <= str.length() ? str.substring(index) : "";
        }
        return str;
    }


    /**
     * 生成分布式ID
     *
     * @param removeMethod 要删除的节点的类型
     * @return
     * @throws Exception
     */
    public  String  generateId(RemoveMethod removeMethod) throws Exception {
        checkRunning();
        final String fullNodePath = root.concat("/").concat(nodeName);
        //利用Zookeeper在高并发场景下产生全局唯一的顺序节点
        final String ourPath = client.createPersistentSequential(fullNodePath, null);
        System.out.println("OurPath: " + ourPath);

        if (removeMethod.equals(RemoveMethod.IMMEDIATELY)) {
            client.delete(ourPath);
        } else if (removeMethod.equals(RemoveMethod.DELAY)) {
            cleanExecutor.execute(new Runnable() {
                public void run() {
                    client.delete(ourPath);
                }
            });
        }
        //node-0000000000, node-0000000001，ExtractId提取ID
        return ExtractId(ourPath);
    }
}
