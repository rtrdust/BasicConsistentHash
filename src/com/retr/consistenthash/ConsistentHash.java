package com.retr.consistenthash;

import java.util.*;
import java.util.zip.CRC32;

public class ConsistentHash {
    // 虚拟节点数量，默认为1
    private int replicas;
    // 用于存储hash节点的map
    private HashMap<Integer, String> nodeMap;
    // 用于存储hash节点的hash排序
    private List<Integer> hashList;
    // 自定义哈希算法
    private HashAlgorithm hashAlgorithm;

    public ConsistentHash() {
        this(1, new HashAlgorithm());
    }

    public ConsistentHash(int replicas) {
        this(replicas, new HashAlgorithm());
    }

    public ConsistentHash(int replicas, HashAlgorithm hashAlgorithm) {
        this.replicas = replicas;
        this.nodeMap = new HashMap<>();
        this.hashAlgorithm = hashAlgorithm;
        this.hashList = new LinkedList<>();
    }

    public void Add(String node) {
        Add(Collections.singletonList(node));
    }

    public void Add(List<String> nodeList) {
        for(String node: nodeList) {
            for(int i = 0; i < replicas; i++) {
                int hash = hashAlgorithm.hash((node + i).getBytes());
                hashList.add(hash);
                nodeMap.put(hash, node);
            }
        }
        hashList.sort(Comparator.comparingInt(o -> o));
    }

    public String Get(String key) {
        if (nodeMap.isEmpty()) {
            return null;
        }

        int hash = hashAlgorithm.hash(key.getBytes());

        boolean foundNode = false;
        int nodeKey = -1;
        for (int nodeHash: hashList) {
            if (nodeHash > hash) {
                nodeKey = nodeHash;
                foundNode = true;
                break;
            }
        }
        if (!foundNode) {
            nodeKey = hashList.get(0);
        }
        return nodeMap.get(nodeKey);
    }
}

class HashAlgorithm {
    public int hash(byte[] data) {
        CRC32 c = new CRC32();
        c.update(data);
        return (int)c.getValue();
    }
}