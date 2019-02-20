package com.serli.oracle.of.bacon.loader.elasticsearch;

import com.fasterxml.jackson.databind.util.JSONPObject;
import com.serli.oracle.of.bacon.repository.ElasticSearchRepository;
import io.searchbox.core.Bulk;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.security.PutRoleMappingResponse;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.node.Node;
import org.elasticsearch.threadpool.ThreadPool;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.concurrent.atomic.AtomicInteger;

public class CompletionLoader {
    private static AtomicInteger count = new AtomicInteger(0);

    public static void main(String[] args) throws IOException, InterruptedException {
        RestHighLevelClient client = ElasticSearchRepository.createClient();

        if (args.length != 1) {
            System.err.println("Expecting 1 arguments, actual : " + args.length);
            System.err.println("Usage : completion-loader <actors file path>");
            System.exit(-1);
        }

        String inputFilePath = args[0];

        // Creating index
        CreateIndexRequest indexRequest = new CreateIndexRequest("bacon");
        client.indices().create(indexRequest, RequestOptions.DEFAULT);


        // Mapping
        PutMappingRequest mappingRequest = new PutMappingRequest("bacon");
        String json = new JSONObject()
                .put("properties", new JSONObject()
                        .put("name", new JSONObject()
                                .put("type", "completion"))).toString();
        mappingRequest.source(json
                , XContentType.JSON
        );
        mappingRequest.type("_doc");
        AcknowledgedResponse response = client.indices().putMapping(mappingRequest, RequestOptions.DEFAULT);


        // Bulk insert
        BulkProcessor bulkProcessor = getBulkProcessor(client);
        try (BufferedReader bufferedReader = Files.newBufferedReader(Paths.get(inputFilePath))) {
            bufferedReader
                    .lines()
                    .forEach(line -> {
                        int index = count.incrementAndGet();
                        if(index >= 2) {
                            IndexRequest request = new IndexRequest("bacon");
                            request.id(Integer.toString(index));
                            request.type("doc");
                            JSONArray inputs = new JSONArray();
                            if(line.split(", ").length == 2) {
                                String name = line.split(", ")[0];
                                String lastname = line.split(", ")[1];
                                inputs.put(name);
                                inputs.put(lastname);
                            }
                            inputs.put(line);
                            String inputJson = new JSONObject().
                                    put("input", inputs).toString();
                            request.source(XContentType.JSON, "name", inputJson);
                            bulkProcessor.add(request);
                        }


                    });
        }
        bulkProcessor.close();
        System.out.println("Inserted total of " + count.get() + " actors");

        client.close();
    }

    public static BulkProcessor getBulkProcessor(RestHighLevelClient client){
        AtomicInteger subcount = new AtomicInteger(0);
        BulkProcessor bulkProcessor = BulkProcessor.builder(client::bulkAsync, new BulkProcessor.Listener() {
            @Override
            public void beforeBulk(long executionId, BulkRequest request) {

            }

            @Override
            public void afterBulk(long executionId, BulkRequest request, BulkResponse response) {
                System.out.println(subcount.addAndGet(request.requests().size()));
            }

            @Override
            public void afterBulk(long executionId, BulkRequest request, Throwable failure) {

            }
        })
                .setBulkActions(1000)
                //.setBulkSize(new ByteSizeValue(5, ByteSizeUnit.MB))
                .setFlushInterval(TimeValue.timeValueSeconds(5))
                .build();

        return bulkProcessor;



    }
}
