package org.pujun;

import com.mongodb.*;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.SimpleTimeZone;

/**
 * Created by Millet on 2016/3/8 0008.
 *
 * Find the data of PM and Meteo which are in the same time point.
 *
 * Warning: Ensure the target collections are empty to avoid repeated data, which may cause exceptions.
 */
public class PmAndMeteo {
    public static void main(String[] args) throws ParseException {
        findSameTimeData();
    }

    public static void findSameTimeData() throws ParseException {
        //链接服务器
        MongoClient client = new MongoClient("127.0.0.1", 27017);
        DB dbMeteo = client.getDB("meteo");
        DB dbPM = client.getDB("pmdata");
        DBCollection dbMeteoCollection = dbMeteo.getCollection("meteo_data");
        DBCollection dbPMCollection = dbPM.getCollection("pmProcess");

        DBCollection dbMeteoSameTimeCollection = dbMeteo.getCollection("meteo_data_same_time");
        DBCollection dbPMSameTimeCollection = dbPM.getCollection("pm_data_same_time");

        //设置时间格式和时区
        SimpleDateFormat df = new SimpleDateFormat(("yyyy-MM-dd HH:mm:ss"));
        df.setCalendar(new GregorianCalendar(new SimpleTimeZone(0, "GMT")));        //查询同样要设置时区

        //循环比较
        Date startDate = df.parse("2013-12-01 00:00:00");
        Date endDate = df.parse("2015-12-31 23:00:00");

        Calendar eachDate = Calendar.getInstance();
        eachDate.setTime(startDate);
        //rightNow.add(Calendar.YEAR, 0);               //年份增减
        //rightNow.add(Calendar.MONTH,0);               //月份增减
        //rightNow.add(Calendar.DAY_OF_YEAR,0);         //天数增减

        while(eachDate.getTime().before(endDate)) {

            eachDate.add(Calendar.HOUR, 1);              //按小时增减循环
            Date date = eachDate.getTime();

            //查询各自集合中 是否包含这一天
            BasicDBObject queryObject = new BasicDBObject();
            queryObject.put("time", date);

            //设置游标
            DBCursor queryCursorMeteo = dbMeteoCollection.find(queryObject);
            DBCursor queryCursorPM = dbPMCollection.find(queryObject);

            //若都包含此天，则把对应的文档分别插入各自新表中
            if ((queryCursorMeteo.hasNext()) && (queryCursorPM.hasNext())) {      //判断是否这一天至少有1天
                while (queryCursorMeteo.hasNext()) {
                    dbMeteoSameTimeCollection.insert(queryCursorMeteo.next());      //存入meteo库的meteo_data_same_time集合
                }
                while (queryCursorPM.hasNext()) {
                    dbPMSameTimeCollection.insert(queryCursorPM.next());            //存入pmdata库的pm_data_same_time集合
                }

            }
        }

    }

}
