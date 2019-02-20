package com.serli.oracle.of.bacon.api;

import com.serli.oracle.of.bacon.repository.ElasticSearchRepository;
import com.serli.oracle.of.bacon.repository.MongoDbRepository;
import com.serli.oracle.of.bacon.repository.Neo4JRepository;
import com.serli.oracle.of.bacon.repository.RedisRepository;
import net.codestory.http.annotations.Get;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

public class APIEndPoint {
    private final Neo4JRepository neo4JRepository;
    private final ElasticSearchRepository elasticSearchRepository;
    private final RedisRepository redisRepository;
    private final MongoDbRepository mongoDbRepository;

    public APIEndPoint() {
        neo4JRepository = new Neo4JRepository();
        elasticSearchRepository = new ElasticSearchRepository();
        redisRepository = new RedisRepository();
        mongoDbRepository = new MongoDbRepository();


    }

    @Get("bacon-to?actor=:actorName")
    public String getConnectionsToKevinBacon(String actorName) {

        this.redisRepository.addSearch(actorName);

        return "[\n" +
                "{\n" +
                "\"data\": {\n" +
                "\"id\": 85449,\n" +
                "\"type\": \"Actor\",\n" +
                "\"value\": \"Bacon, Kevin (I)\"\n" +
                "}\n" +
                "},\n" +
                "{\n" +
                "\"data\": {\n" +
                "\"id\": 2278636,\n" +
                "\"type\": \"Movie\",\n" +
                "\"value\": \"Mystic River (2003)\"\n" +
                "}\n" +
                "},\n" +
                "{\n" +
                "\"data\": {\n" +
                "\"id\": 1394181,\n" +
                "\"type\": \"Actor\",\n" +
                "\"value\": \"Robbins, Tim (I)\"\n" +
                "}\n" +
                "},\n" +
                "{\n" +
                "\"data\": {\n" +
                "\"id\": 579848,\n" +
                "\"source\": 85449,\n" +
                "\"target\": 2278636,\n" +
                "\"value\": \"PLAYED_IN\"\n" +
                "}\n" +
                "},\n" +
                "{\n" +
                "\"data\": {\n" +
                "\"id\": 9985692,\n" +
                "\"source\": 1394181,\n" +
                "\"target\": 2278636,\n" +
                "\"value\": \"PLAYED_IN\"\n" +
                "}\n" +
                "}\n" +
                "]";
    }

    @Get("suggest?q=:searchQuery")
    public List<String> getActorSuggestion(String searchQuery) throws IOException {
        return elasticSearchRepository.getActorsSuggests(searchQuery);
    }

    @Get("last-searches")
    public List<String> last10Searches() {
        return this.redisRepository.getLastTenSearches();
    }

    @Get("actor?name=:actorName")
    public String getActorByName(String actorName) {
        return mongoDbRepository.getActorByName(actorName).toString();
    }
}
