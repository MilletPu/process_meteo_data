package org.pujun;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.SimpleTimeZone;

/**
 * Created by Millet on 2016/3/8 0008.

 * Process the original meteorological data(selected) into MongoDB.
 * Including:  USAF, WBAN, TIME, DIR, SPD, TEMP, DEWP, SLP, STP.
 *
 * Warning: This code can be only used to specific data, as different data is in different types.
 * Warning: This code is of low tolerance and susceptible to errors.
 */
public class MeteoData {
    public static void main(String[] args) {
        processMeteoData();
    }

    public static void processMeteoData(){
        //链接数据库
        MongoClient client = new MongoClient("127.0.0.1", 27017);
        MongoDatabase db = client.getDatabase("testcode");
        MongoCollection dbCollection = db.getCollection("meteo_data");

        //读入文件
        BufferedReader bufferedReader = null;
        try {
            bufferedReader = new BufferedReader(new FileReader("E:\\MyJava\\meteo1.txt"));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }

        try {
            bufferedReader.readLine();      //读取第一行，不使用
        } catch (IOException e) {
            e.printStackTrace();
        }

        String line = null;
        try {
            while((line = bufferedReader.readLine()) != null){      //开始读取正式数据
                int usaf = Integer.parseInt(line.substring(0,6).trim());        //读取USAF台站号
                int wban = Integer.parseInt(line.substring(7,12).trim());       //读取WBAN台站号

                double dir,spd,temp,dewp,slp,stp;
                if(line.substring(26,29).trim().matches("[*]+")){
                    dir = -1;                   //读取风向，缺失置-1
                }else{
                    dir = Double.parseDouble(line.substring(26,29).trim());
                }

                if(line.substring(30,33).trim().matches("[*]+")){
                    spd = -1;                   //读取风速，缺失置-1
                }else{
                    spd = Double.parseDouble(line.substring(30,33).trim());
                }

                if(line.substring(83,87).trim().matches("[*]+")){
                    temp = -1;                  //读取温度，缺失置-1
                }else{
                    temp = Double.parseDouble(line.substring(83,87).trim());
                }

                if(line.substring(88,92).trim().matches("[*]+")){
                    dewp = -1;                  //读取露点，缺失置-1
                }else{
                    dewp = Double.parseDouble(line.substring(88,92).trim());
                }

                if(line.substring(93,99).trim().matches("[*]+")){
                    slp = -1;                   //读取海平面气压，缺失置-1
                }else{
                    slp = Double.parseDouble(line.substring(93,99).trim());
                }

                if(line.substring(106,112).trim().matches("[*]+")){
                    stp = -1;                   //读取当地气压，缺失置-1
                }else{
                    stp = Double.parseDouble(line.substring(106,112).trim());
                }

                String time = line.substring(13,25).trim();     //获取时间字符串
                time = time.substring(0,4) + "-" + time.substring(4,6) + "-" + time.substring(6,8) + " "
                        + time.substring(8,10) + ":" + time.substring(10,12) +":00";    //凑成可用的时间字符串
                SimpleDateFormat df = new SimpleDateFormat(("yyyy-MM-dd HH:mm:ss"));    //设置字符串时间格式
                df.setCalendar(new GregorianCalendar(new SimpleTimeZone(0, "GMT")));    //设置时区，否则Mongo会自动减去8小时以GMT时间储存
                Date date = df.parse(time);                     //date对象成功

                //df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                //String newDate = df.format(date);
                //SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z' Z");       //时间以Date()存储

                Document meteo = new Document();
                meteo.append("usaf", usaf);
                meteo.append("wban", wban);
                meteo.append("time", date);
                meteo.append("dir", dir);
                meteo.append("spd", spd);
                meteo.append("temp", temp);
                meteo.append("dewp", dewp);
                meteo.append("slp", slp);
                meteo.append("stp", stp);
                dbCollection.insertOne(meteo);

            }
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ParseException e) {
            e.printStackTrace();
        }

    }

}
