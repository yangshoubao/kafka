package com.kentrasoft.kafka.controller;

import com.alibaba.fastjson.JSONObject;
import com.kentrasoft.kafka.service.KafkaProducer;
import com.kentrasoft.kafka.util.ProfileUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import javax.servlet.http.HttpServletRequest;
import java.io.*;
import java.util.List;
import java.util.concurrent.TimeUnit;

@RestController
@RequestMapping("kafka")
public class KafkaController {

    private Log log = LogFactory.getLog(KafkaController.class);

    @Autowired
    private KafkaProducer kafkaProducer;

    @Autowired
    private StringRedisTemplate stringRedisTemplate;

    @RequestMapping("send.do")
    public Object send(HttpServletRequest request){

        JSONObject resultObject = new JSONObject();
        InputStream is = null;
        //获取请求参数
        try{
            is = request.getInputStream();
            //读取输入流，即参数内容
            StringBuffer sbResult = new StringBuffer();
            BufferedReader br = new BufferedReader(new InputStreamReader(is));
            String line = "";
            while ((line = br.readLine()) != null) {
                sbResult.append(line);
            }
            log.info("接收上报数据：" +sbResult);
            if(sbResult.length() <= 0){
                log.error("接收上报数据为空");
                resultObject.put("code", -1);
                resultObject.put("message", "数据为空");
                return resultObject;
            }

            JSONObject jsonObject = JSONObject.parseObject(sbResult.toString());

            String data = jsonObject.getJSONObject("service").getJSONObject("data").getString("data");
            String jsonString = jsonObject.getJSONObject("service").getJSONObject("data").getString("jsonString");

            if(StringUtils.isNotBlank(data)){ // 成都上报数据
                kafkaProducer.sendToCdUpload(sbResult.toString());
            }else if(StringUtils.isNotBlank(jsonString)){ // 爱联上报数据
                // 解析数据
                List<JSONObject> dataList = ProfileUtil.profile(jsonString);
                for(JSONObject jsonObjectData : dataList){
                    jsonObject.getJSONObject("service").put("data", jsonObjectData);
                    if(log.isInfoEnabled()){
                        log.info("解析后的上报数据：" +jsonObject.toJSONString());
                    }
                    String deviceId = jsonObject.getString("deviceId");
                    String timestamp = jsonObject.getJSONObject("service").getJSONObject("data").getString("stamp");
                    String typeIdent = jsonObject.getJSONObject("service").getJSONObject("data").getString("typeIdent");
                    String agpsIdent = jsonObject.getJSONObject("service").getJSONObject("data").getString("agpsIdent");

                    if(StringUtils.isNotBlank(deviceId) && StringUtils.isNotBlank(timestamp)){
                        String key = deviceId + "-" + timestamp;
                        String dataRedis = stringRedisTemplate.opsForValue().get(key);
                        if(dataRedis == null || "".equals(dataRedis)){
                            stringRedisTemplate.opsForValue().set(key, sbResult.toString(), 24, TimeUnit.HOURS);
                            kafkaProducer.sendToUpload(jsonObject.toString());
                        }
                    }else{
                        if(typeIdent != null && typeIdent.equals("ACK") && agpsIdent != null && agpsIdent.equals("agps")){
                            kafkaProducer.sendToAgps(jsonObject.toString());
                        }else{
                            kafkaProducer.sendToUpload(jsonObject.toString());
                        }
                    }
                }
            }
        }catch (Exception e) {
            e.printStackTrace();
        }finally{
            if(is != null){
                try {
                    is.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

        resultObject.put("code", 0);
        resultObject.put("message", "成功");
        return resultObject;
    }

    @RequestMapping("sendToCallback.do")
    public Object sendToCallback(HttpServletRequest request){

        JSONObject resultObject = new JSONObject();
        InputStream is = null;
        //获取请求参数
        try{
            is = request.getInputStream();
            //读取输入流，即参数内容
            StringBuffer sbResult = new StringBuffer();
            BufferedReader br = new BufferedReader(new InputStreamReader(is));
            String line = "";
            while ((line = br.readLine()) != null) {
                sbResult.append(line);
            }
            log.info("接收回调数据：" +sbResult);
            if (sbResult.length() <= 0){
                log.error("接收回调数据为空");
                resultObject.put("code", -1);
                resultObject.put("message", "数据为空");
                return resultObject;
            }
            kafkaProducer.sendToCallback(sbResult.toString());
        }catch (Exception e) {
            e.printStackTrace();
        }finally{
            if(is != null){
                try {
                    is.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

        resultObject.put("code", 0);
        resultObject.put("message", "成功");
        return resultObject;
    }

    @RequestMapping("subscribe.do")
    public Object subscribe(HttpServletRequest request){

        JSONObject resultObject = new JSONObject();
        InputStream is = null;
        //获取请求参数
        try{
            is = request.getInputStream();
            //读取输入流，即参数内容
            StringBuffer sbResult = new StringBuffer();
            BufferedReader br = new BufferedReader(new InputStreamReader(is));
            String line = "";
            while ((line = br.readLine()) != null) {
                sbResult.append(line);
            }
            log.info("接收设备绑定激活回调数据：" +sbResult);
            if (sbResult.length() <= 0){
                log.error("接收设备绑定激活回调数据为空");
                resultObject.put("code", -1);
                resultObject.put("message", "数据为空");
                return resultObject;
            }
            kafkaProducer.sendToSubscribe(sbResult.toString());
        }catch (Exception e) {
            e.printStackTrace();
        }finally{
            if(is != null){
                try {
                    is.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

        resultObject.put("code", 0);
        resultObject.put("message", "成功");
        return resultObject;
    }

    @RequestMapping("sendTestUpload.do")
    private Object sendTestUpload(String param){
        kafkaProducer.sendToUpload(param);
        return "upload";
    }

    @RequestMapping("sendTestCallback.do")
    private Object sendTestCallback(String param){
        kafkaProducer.sendToCallback(param);
        return "callback";
    }

    @RequestMapping("test.do")
    private Object test(){
        log.info("执行test方法");
        return "test";
    }

}
