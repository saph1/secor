package com.pinterest.secor.timestamp;

import kafka.message.MessageAndMetadata;
import kafka.message.MessageAndOffset;
//import com.google.protobuf.Message;

import com.pinterest.secor.util.ProtobufUtil;
import com.pinterest.secor.message.Message;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;


import org.slf4j.Logger;

import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;





public class KafkaProtoMessageTimestamp implements KafkaMessageTimestamp {

        String timestampFieldName = "timestamp";
        String timestampFieldSeparator = ".";
        private static final Logger LOG = LoggerFactory.getLogger(ProtobufUtil.class);

      @Override
    public long getTimestamp(MessageAndMetadata<byte[], byte[]> kafkaMessage) {

        LOG.info("Using protobuf timestamp field path: {} with separator: {}", timestampFieldName,timestampFieldSeparator);
        com.google.protobuf.Message result= null;
        Long resultFinal= 0L;

        try{

        Class<? extends com.google.protobuf.Message> messageClass = (Class<? extends com.google.protobuf.Message>) Class.forName("fr.tradelab.slld.proto.Message$Feed");

        Method messageParseMethod = messageClass.getDeclaredMethod("parseFrom",new Class<?>[] { byte[].class });

        result = (com.google.protobuf.Message) messageParseMethod.invoke(null,kafkaMessage.message() );

        resultFinal =(Long) result.getField(result.getDescriptorForType().findFieldByName(timestampFieldName));

        }



    catch (ClassNotFoundException e) {
                LOG.error("Unable to load protobuf message class", e);
            } catch (NoSuchMethodException e) {
                LOG.error("Unable to find parseFrom() method in protobuf message class", e);
            } catch (SecurityException e) {
                LOG.error("Unable to use parseFrom() method from protobuf message class", e);
                        } catch (IllegalAccessException e) {
            throw new RuntimeException("Can't parse protobuf message, since parseMethod() is not accessible. "
                    + "Please check your protobuf version (this code works with protobuf >= 2.6.1)", e);
                        }  catch (InvocationTargetException e) {
            throw new RuntimeException("Error parsing protobuf message", e);
                        }

        if (resultFinal != null)
        return resultFinal;
        else return 0L;
    }





    @Override
    public long getTimestamp(MessageAndOffset messageAndOffset) {
        return 0l;
    }
}
