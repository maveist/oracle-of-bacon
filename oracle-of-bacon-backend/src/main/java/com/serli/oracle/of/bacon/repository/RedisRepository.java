package com.serli.oracle.of.bacon.repository;

import redis.clients.jedis.Jedis;

import java.util.List;
import java.util.ArrayList;
import java.util.Arrays;

public class RedisRepository {
    private static String KEY = "tenresearch";

    private final Jedis jedis;

    public RedisRepository() {

        this.jedis = new Jedis("localhost");

    }

    public void addSearch(String name){
        if(!jedis.exists(KEY)){
            jedis.lpush(KEY, name);
        }else {
            List<String> searches = jedis.lrange(KEY, 0, -1);
            if (searches.size() == 10) {
                jedis.lpush(KEY, name);
                jedis.rpop(KEY);
            }
        }
    }

    public List<String> getLastTenSearches() {
        // TODO implement last 10 searchs
        return jedis.lrange(KEY, 0, -1);
    }
}
