package org.pujun;

import com.mongodb.*;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

/**
 * Created by Millet on 2016/3/8 0008.

 * Process the original meteorological data(unselected) into MongoDB. All the data is stored in the type of String.
 *
 * Warning: This code can be only used to specific data, as different data is in different types.
 * Warning: This code is of low tolerance and susceptible to errors.
 */
public class MeteoOriginalData {
    public static void main(String[] args) throws IOException {
        processMeteoOriginalData();
    }

    public static void processMeteoOriginalData(){
        //连接服务器
        MongoClient client = new MongoClient("127.0.0.1", 27017);
        MongoDatabase db = client.getDatabase("testcode");
        MongoCollection dbCollection = db.getCollection("meteo_original_data");

        //读取文件
        BufferedReader bufferedReader = null;
        try {
            bufferedReader = new BufferedReader(new FileReader("E:\\MyJava\\meteo1.txt"));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }

        String[] keys = new String[0];
        try {
            keys = bufferedReader.readLine().split("[ ]+");         //读取第一行keys，以空格划分
        } catch (IOException e) {
            e.printStackTrace();
        }


        String line = null;                                         //开始读取数据values，从第二行至最后一行，以空格划分
        try {
            while((line = bufferedReader.readLine()) != null){
                String[] values = line.split("[ ]+");
                Document meteo = new Document();

                for(int i=0; i<keys.length; i++){
                    meteo.append(keys[i].toLowerCase(), values[i]); //加入k-v对
                }

                dbCollection.insertOne(meteo);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        //因不需要使用这个数据，所以未做各数据的类型转换，全部为String
    }
}
