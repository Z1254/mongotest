package com.autohome;

import com.mongodb.MongoClient;
import com.mongodb.ServerAddress;
import com.mongodb.WriteConcern;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.changestream.ChangeStreamDocument;
import com.mongodb.client.model.changestream.FullDocument;
import com.mongodb.client.model.changestream.OperationType;
import org.bson.Document;

import java.nio.channels.Channel;
import java.util.ArrayList;
import java.util.List;

/**
 * @author zzn
 * @date 2020/2/23 21:08
 */
public class Test {
    private static MongoCollection<Document> studentCollection;
    private static MongoCollection<Document> studentIncrCollection;

    static {
        List<ServerAddress> addresses = new ArrayList<ServerAddress>();
        ServerAddress address1 = new ServerAddress("10.168.0.67", 27017);
        ServerAddress address2 = new ServerAddress("10.168.0.68", 27017);
        ServerAddress address3 = new ServerAddress("10.168.0.69", 27017);
        addresses.add(address1);
        addresses.add(address2);
        addresses.add(address3);
        MongoClient client = new MongoClient(addresses);
        MongoDatabase test = client.getDatabase("test");
        studentCollection = test.getCollection("Student");
        studentIncrCollection = test.getCollection("IncrStudent");
    }

    public static void main(String[] args) {
        //preInsertData();
        //开启监听任务
        MongoCursor<ChangeStreamDocument<Document>> iterator = studentCollection.watch().fullDocument(FullDocument.UPDATE_LOOKUP).iterator();
        while (iterator.hasNext()){
            ChangeStreamDocument<Document> changeEvent = iterator.next();
            OperationType type = changeEvent.getOperationType();
            System.out.println(type);
            if (type == OperationType.INSERT || type == OperationType.DELETE || type == OperationType.UPDATE || type == OperationType.REPLACE){
                Document incrDocc = new Document();
                incrDocc.append("filed_key",changeEvent.getDocumentKey().get("_id"));
                incrDocc.append("filed_date",changeEvent.getFullDocument());
                studentIncrCollection.insertOne(incrDocc);
            }
        }
    }

    /**
     * description: 分批写入数据
     *
     * @param
     * @return void
     */
    private static void preInsertData() {
        int current = 0;
        int batchSize = 100;
        while (current < 1000) {
            List<Document> documents = new ArrayList<Document>();
            for (int i = 0; i < batchSize; i++) {
                Document document = new Document();
                document.append("id", String.valueOf(System.currentTimeMillis()));
                document.append("name", "张三" + i);
                documents.add(document);
            }
            studentCollection.insertMany(documents);
            current += batchSize;
        }
    }
}
