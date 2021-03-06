package com.kentrasoft.kafka.util;

import com.alibaba.fastjson.JSONObject;

import java.util.ArrayList;
import java.util.List;

public class ProfileUtil {

    public static List<JSONObject> profile(String inputs){

        List<JSONObject> resultList = new ArrayList<>();

        String[] inputArr = inputs.split(";");

        for(String input : inputArr){
            // 协议前缀
            String agreement = null;
            // imei
            // private String sImei;
            // 数据长度
            String slong = null;
            // 命令类型
            String typeIdent = null;
            // 日期
            String dateIdent = null;
            // 时间
            String timeIdent = null;
            // A,gps 定位有效
            String effective = null;
            // 纬度
            String latitude = null;
            // N,纬度表示
            String latitudeIdent = null;
            // 经度
            String longitude = null;
            // E,经度表示
            String longitudeIdent = null;
            // 速度
            String speed = null;
            // 方向à没有方向用-1表示
            // private String direction;
            // 海拔
            String altitude = null;
            // 卫星个数
            int satellite = 0;
            // 预留à0
            String reserve1 = null;
            // 电量
            String quantity = null;
            // 预留à0
            String reserve2 = null;
            // MAC
            String macIdent = null;
            // 计步
            String paceIdent = null;
            // 时间戳
            String stamp = null;
            // 固件编译日期
            String compile = null;
            // 固件版本
            String servion = null;
            // GPS室内室外
            String inOut = null;
            //agps
            String agpsIdent = null;
            //包数
            String numberIdent = null;

            JSONObject jsonObject = new JSONObject();
            try {
                String jsonStr = input.toString();
                String[] jsonList = jsonStr.split(",");
                String paramOne = jsonList[0];
                String paramTwo =  jsonList[1];
                if(paramOne.equals("ACK")&&paramTwo.equals("agps")){
                    //ACK,agps,0
                    typeIdent = paramOne;
                    agpsIdent = paramTwo;
                    numberIdent = jsonList[2];
                    jsonObject.put("typeIdent", typeIdent);
                    jsonObject.put("agpsIdent", agpsIdent);
                    jsonObject.put("numberIdent", numberIdent);
                    resultList.add(jsonObject);
                    return resultList;
                }
                String[] one = paramOne.split("\\*");
                // 一号位
                agreement = one[0];
                slong = one[1];
                typeIdent = one[2];
                if (typeIdent.equals("AL_ACK")) {
                    jsonObject.put("agreement", agreement);
                    jsonObject.put("slong", slong);
                    jsonObject.put("typeIdent", typeIdent);
                    resultList.add(jsonObject);
                    return resultList;
                }
                dateIdent = paramTwo;
                timeIdent = jsonList[2];
                effective = jsonList[3];
                latitude = jsonList[4];
                latitudeIdent = jsonList[5];
                longitude = jsonList[6];
                longitudeIdent = jsonList[7];
                speed = jsonList[8];
                altitude = jsonList[9];
                satellite = Integer.valueOf(jsonList[10]);
                reserve1 = jsonList[11];
                quantity = jsonList[12];
                reserve2 = jsonList[13];
                macIdent = jsonList[14];
                paceIdent = jsonList[15];
                stamp = jsonList[16];
                compile = jsonList[17];
                servion = jsonList[18];
                inOut = jsonList[19];
            } catch (Exception e) {
                agreement = input.toString();
            }
            jsonObject.put("agreement", agreement);
            jsonObject.put("slong", slong);
            jsonObject.put("typeIdent", typeIdent);
            jsonObject.put("dateIdent", dateIdent);
            jsonObject.put("timeIdent", timeIdent);
            jsonObject.put("effective", effective);
            jsonObject.put("latitude", latitude);
            jsonObject.put("latitudeIdent", latitudeIdent);
            jsonObject.put("longitude", longitude);
            jsonObject.put("longitudeIdent", longitudeIdent);
            jsonObject.put("speed", speed);
            jsonObject.put("altitude", altitude);
            jsonObject.put("satellite", satellite);
            jsonObject.put("reserve1", reserve1);
            jsonObject.put("quantity", quantity);
            jsonObject.put("reserve2", reserve2);
            jsonObject.put("macIdent", macIdent);
            jsonObject.put("paceIdent", paceIdent);
            jsonObject.put("stamp", stamp);
            jsonObject.put("compile", compile);
            jsonObject.put("servion", servion);
            jsonObject.put("inOut", inOut);
            jsonObject.put("agpsIdent", agpsIdent);
            jsonObject.put("numberIdent", numberIdent);

            resultList.add(jsonObject);
        }

        return resultList;
    }
}
