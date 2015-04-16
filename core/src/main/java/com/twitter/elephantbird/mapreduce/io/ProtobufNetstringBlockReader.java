package com.twitter.elephantbird.mapreduce.io;

import com.google.protobuf.ByteString;
import com.google.protobuf.Message;
import com.twitter.elephantbird.util.Protobufs;
import com.twitter.elephantbird.util.StreamSearcher;
import com.twitter.elephantbird.util.TypeRef;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;

public class ProtobufNetstringBlockReader<M extends Message> extends BinaryBlockReader<M> {

  private static final Logger LOG = LoggerFactory.getLogger(ProtobufNetstringBlockReader.class);

  public static final byte[] SENTINEL = new byte[]{(byte) 0xA5, (byte) 0xA5};

  public ProtobufNetstringBlockReader(InputStream in, TypeRef<M> typeRef) {
    super(in, ProtobufConverter.newInstance(typeRef));
    LOG.info("ProtobufNetstringReader, my typeClass is " + typeRef.getRawClass());
  }

  public boolean readProtobuf(ProtobufWritable<M> message) throws IOException {
    return readNext(message);
  }

  public boolean readProtobufBytes(BytesWritable message) throws IOException {
    return readNextProtoBytes(message);
  }

  /**
   * Finds next block marker and reads the block. If skipIfStartingOnBoundary is set
   * skips the the first block if the marker starts exactly at the current position.
   * (i.e. there were no bytes from previous block before the start of the marker).
   */
  public List<ByteString> parseNextBlock(boolean skipIfStartingOnBoundary) throws IOException {
    LOG.debug("BlockReader: none left to read, skipping to sync point");
    long skipped = skipToNextSyncPoint();
    if (skipped <= -1) {
      LOG.debug("BlockReader: SYNC point eof");
      // EOF if there are no more sync markers.
      return null;
    }

    int blockSize = readInt();
    LOG.debug("BlockReader: found sync point, next block has size " + blockSize);
    if (blockSize < 0) {
      LOG.debug("ProtobufReader: reading size after sync point eof");
      // EOF if the size cannot be read.
      return null;
    }


    byte[] buff = new byte[blockSize + SENTINEL.length];
    IOUtils.readFully(in_, buff, 0, buff.length);

    // Remove the SENTINEL value
    byte[] byteArray = new byte[blockSize];
    for(int i = 0; i < byteArray.length; i++) {
      byteArray[i] = buff[i];
    }

    if (skipIfStartingOnBoundary && skipped == Protobufs.KNOWN_GOOD_POSITION_MARKER.length) {
      // skip the current current block
      return parseNextBlock(false);
    }

    SerializedBlock block = SerializedBlock.parseFrom(byteArray);

    curBlobs_ = block.getProtoBlobs();
    numLeftToReadThisBlock_ = curBlobs_.size();
    LOG.debug("ProtobufReader: number in next block is " + numLeftToReadThisBlock_);
    return curBlobs_;
  }
}
