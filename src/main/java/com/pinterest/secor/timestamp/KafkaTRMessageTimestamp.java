package com.pinterest.secor.timestamp;


import java.text.ParseException;
import kafka.message.MessageAndMetadata;
import kafka.message.MessageAndOffset;
import net.minidev.json.JSONObject;
import net.minidev.json.JSONValue;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;

import java.text.SimpleDateFormat;
import java.util.Date;

import kafka.message.Message;
//import com.pinterest.secor.message.Message;


public class KafkaMMessageTimestamp implements KafkaMessageTimestamp {
	
	private String tradelab_timestamp[] = {"timestamp"}; 
	
	
	 protected static long toMillis(final long timestamp) {
	        final long nanosecondDivider = (long) Math.pow(10, 9 + 9);
	        final long microsecondDivider = (long) Math.pow(10, 9 + 6);
	        final long millisecondDivider = (long) Math.pow(10, 9 + 3);
	        long timestampMillis;
	        if (timestamp / nanosecondDivider > 0L) {
	            timestampMillis = timestamp / (long) Math.pow(10, 6);
	        } else if (timestamp / microsecondDivider > 0L) {
	            timestampMillis = timestamp / (long) Math.pow(10, 3);
	        } else if (timestamp / millisecondDivider > 0L) {
	            timestampMillis = timestamp;
	        } else {  // assume seconds
	            timestampMillis = timestamp * 1000L;
	        }
	        return timestampMillis;
	}

    @Override    
    public long getTimestamp(MessageAndMetadata<byte[], byte[]> kafkaMessage) {

                   

            JSONObject jsonObject = (JSONObject) JSONValue.parse(kafkaMessage.message());                
		    JSONObject br_obj = (JSONObject) jsonObject.get("bid_request");

		    Date date=new Date();
		    Object oj = br_obj.get("timestamp");
		    String someDate = oj.toString().split(" ")[0];
		    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
		    
		    try{
		      date = sdf.parse(someDate);
		    	}
		    	catch (ParseException e) {
					e.printStackTrace();
					return 0L;
		    	}
		
   
            return date.getTime()/1000L;
        }




    @Override
    public long getTimestamp(MessageAndOffset messageAndOffset) {
        return 0L;
    }
    
   

   
    
    
    
}
