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
 *
 * Process the data of meteorological observation stations: insert them into MongoDB.
 *
 * Warning: This code can be only used to specific data, as different data is in different types.
 * Warning: This code is of low tolerance and susceptible to errors.
 */
public class MeteoStations {
    public static void main(String[] args){
        processStation();
    }

    public static void processStation(){
        MongoClient client = new MongoClient("127.0.0.1", 27017);           //链接服务器
        MongoDatabase db = client.getDatabase("testcode");                  //获取数据库
        MongoCollection dbCollection = db.getCollection("meteo_stations");  //获取要操作的集合

        BufferedReader bufferedReader = null;
        try {
            bufferedReader = new BufferedReader(new FileReader("E:\\MyJava\\meteo_stations.txt"));
            bufferedReader.readLine();                  //读取第一行key，不使用
            bufferedReader.readLine();                  //读取第二行横线，不使用
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }


        String line = null;                             //开始读取正式数据，使用line读取每行数据
        try {
            while ((line = bufferedReader.readLine()) != null) {
                int usaf = Integer.parseInt(line.substring(0, 6).trim());       //读取usaf码，int
                int wban = Integer.parseInt(line.substring(7, 12).trim());      //读取wban码，int
                String name = line.substring(13, 43).trim();                    //读取站点name，注意name每个单词之间存在空格，不可以空格划分
                String d = line.substring(44, line.length()).trim();            //读取name之后的所有字段（国家，维度，经度，海拔），可以空格划分
                String[] strs = d.split("[ ]+");                                //使用正则regex以空格划分数据
                String country = strs[0];                                       //读取国家
                double lat = Double.parseDouble(strs[1]);                       //读取纬度，double
                double lon = Double.parseDouble(strs[2]);                       //读取经度，double
                double ele = Double.parseDouble(strs[3]);                       //读取海拔高度，double

                Document station = new Document();      //新建一个Bson文档
                station.append("usaf", usaf);           //k-v对加入文档
                station.append("wban", wban);
                station.append("name", name);
                station.append("country", country);
                station.append("lat", lat);
                station.append("lon", lon);
                station.append("ele", ele);
                dbCollection.insertOne(station);        //把文档插入集合
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

}
