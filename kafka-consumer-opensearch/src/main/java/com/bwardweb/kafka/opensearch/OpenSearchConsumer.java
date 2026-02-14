package com.bwardweb.kafka.opensearch;

import com.google.gson.JsonParser;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.DefaultConnectionKeepAliveStrategy;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.opensearch.action.bulk.BulkRequest;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.client.indices.GetIndexRequest;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.RestClient;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.client.indices.CreateIndexRequest;
import org.opensearch.common.xcontent.XContentType;

import java.io.IOException;
import java.net.URI;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

@Slf4j
public class OpenSearchConsumer {
    public static void main(String[] args) throws IOException {
        //Create OpenSearch Client
        RestHighLevelClient restHighLevelClient = createOpenSearchClient();

        //Create Kafka Client
        KafkaConsumer<String,String> consumer = createKafkaConsumer();

        log.info("Getting reference to the main thread to control shutdown");
        final Thread mainThread = Thread.currentThread();

        log.info("Adding shutdown hook");
        Runtime.getRuntime().addShutdownHook(new Thread(){
            public void run(){
                log.info("Shutdown exiting: calling callout.wakeup");
                consumer.wakeup();

                log.info("Joingin main thread to allow execution of code in main thread");
                try {
                    mainThread.join();
                }catch (InterruptedException ex){
                    ex.printStackTrace();
                }

            }
        });

        try(restHighLevelClient; consumer) {
            //We need to create index on opensearch if it doesn't exist
            boolean indexExists = restHighLevelClient.indices().exists(new GetIndexRequest("wikimedia"), RequestOptions.DEFAULT);

            if(!indexExists) {
                CreateIndexRequest createIndexRequest = new CreateIndexRequest("wikimedia");
                restHighLevelClient.indices().create(createIndexRequest, RequestOptions.DEFAULT);
                log.info("Create Index Success");
            }else{
                log.info("Index already exists");
            }

            consumer.subscribe(Collections.singleton("wikimedia"));

            while(true){
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(3000));

                int recordCount = records.count();
                log.info("Recieved: " + recordCount + " records");

                BulkRequest bulkRequest = new BulkRequest();

                for(ConsumerRecord<String, String> record : records){
                    //Send record into opensearch

                    //Strategy 1 - define id using kafka record coordinates
                    //String id = record.topic() + '_' + record.partition() + '_' + record.offset();

                    //Strategy 2 - extract id from JSON
                    String id = extractId(record.value());

                    try {
                        IndexRequest indexRequest = new IndexRequest("wikimedia")
                                .source(record.value(), XContentType.JSON)
                                .id(id);
                        //IndexResponse response = restHighLevelClient.index(indexRequest, RequestOptions.DEFAULT);

                        bulkRequest.add(indexRequest);
                    }catch (Exception e){
                        log.error("Error in inserting record", e);
                    }
                }

                if(bulkRequest.numberOfActions() > 0) {
                    BulkResponse response = restHighLevelClient.bulk(bulkRequest, RequestOptions.DEFAULT);

                    log.info("Inserted: " + response.getItems().length + " records");

                    try{
                        Thread.sleep(1000);
                    }catch (InterruptedException e){
                        log.error("Interrupted");
                    }
                }

                consumer.commitSync();
                log.info("Offsets have been committed");
            }

        }catch (WakeupException ex){
            log.info("Consumer is starting to shutdown");
        }catch (Exception ex){
            log.info("Unexpected exception, ex");
        }finally {
            consumer.close();
            restHighLevelClient.close();
            log.info("Consumer shutdown gracefully");
        }

    }

    private static String extractId(String json){
        return JsonParser.parseString(json)
                .getAsJsonObject()
                .get("meta")
                .getAsJsonObject()
                .get("id")
                .getAsString();
    }

    public static RestHighLevelClient createOpenSearchClient() {
        String connString = "http://localhost:9200";

        // we build a URI from the connection string
        RestHighLevelClient restHighLevelClient;
        URI connUri = URI.create(connString);
        // extract login information if it exists
        String userInfo = connUri.getUserInfo();

        if (userInfo == null) {
            // REST client without security
            restHighLevelClient = new RestHighLevelClient(RestClient.builder(new HttpHost(connUri.getHost(), connUri.getPort(), "http")));

        } else {
            // REST client with security
            String[] auth = userInfo.split(":");

            CredentialsProvider cp = new BasicCredentialsProvider();
            cp.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(auth[0], auth[1]));

            restHighLevelClient = new RestHighLevelClient(
                    RestClient.builder(new HttpHost(connUri.getHost(), connUri.getPort(), connUri.getScheme()))
                            .setHttpClientConfigCallback(
                                    httpAsyncClientBuilder -> httpAsyncClientBuilder.setDefaultCredentialsProvider(cp)
                                            .setKeepAliveStrategy(new DefaultConnectionKeepAliveStrategy())));


        }

        return restHighLevelClient;
    }

    private static KafkaConsumer<String,String> createKafkaConsumer() {
        String groupId = "my-java-application";
        String topic = "consumer-opensearch-demo";

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("key.deserializer", StringDeserializer.class.getName());
        properties.setProperty("value.deserializer", StringDeserializer.class.getName());
        properties.setProperty("group.id", groupId);
        properties.setProperty("auto.offset.reset", "latest");
        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

        return new KafkaConsumer<>(properties);

    }
}
