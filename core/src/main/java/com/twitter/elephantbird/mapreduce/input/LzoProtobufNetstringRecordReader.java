package com.twitter.elephantbird.mapreduce.input;

import com.google.protobuf.Message;
import com.twitter.elephantbird.mapreduce.io.ProtobufBlockReader;
import com.twitter.elephantbird.mapreduce.io.ProtobufNetstringBlockReader;
import com.twitter.elephantbird.mapreduce.io.ProtobufWritable;
import com.twitter.elephantbird.util.TypeRef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LzoProtobufNetstringRecordReader<M extends Message>  extends LzoBinaryBlockRecordReader<M, ProtobufWritable<M>> {
  private static final Logger LOG = LoggerFactory.getLogger(LzoProtobufNetstringRecordReader.class);

  public LzoProtobufNetstringRecordReader(TypeRef<M> typeRef) {
    super(typeRef, new ProtobufNetstringBlockReader<M>(null, typeRef), new ProtobufWritable<M>(typeRef));
    LOG.info("LzoProtobufNetstringRecordReader, type args are " + typeRef.getRawClass());
  }
}