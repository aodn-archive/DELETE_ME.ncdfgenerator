package au.org.emii.wps;

import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


class StreamAdaptor extends InputStream
{
    class HelperByteArrayOutputStream extends ByteArrayOutputStream {
        byte [] getInternalBuffer() {
            return buf;
        }
    }

    private static final int CLIENT_READ_SIZE_HINT = 8192;
    private static final Logger logger = LoggerFactory.getLogger(StreamAdaptor.class);
    private final StreamAdaptorSource source;
    private final HelperByteArrayOutputStream bos;
    private int readPos;

    StreamAdaptor(StreamAdaptorSource source) throws Exception {
        this.source = source;
        this.bos = new HelperByteArrayOutputStream();
        source.prepare(bos);
        readPos = 0;
    }

    public int available() {
        return CLIENT_READ_SIZE_HINT;
    }

    public void close() {
        // TODO Geoserver should be calling this, if the connection gets terminated by http client!!!!
        logger.info("close() called");
        source.close();
    }

    public int read() throws IOException {
        // inefficient, but sane clients will call read(buf,off,len) instead
        byte [] buf = new byte [1];
        if (read(buf, 0, 1) > 0) {
            return buf[0];
        } else {
            return -1;
        }
    }

    public int read(byte[] dst, int off, int len) throws IOException {

        // request exceeds what's available in the buffer
        if (readPos + len >= bos.size()) {

            // re-align the ByteArray internal buffer by discarding already consumed data
            if (readPos != 0) {
                int remainingSize = bos.size() - readPos;
                byte [] remaining = new byte[remainingSize];
                System.arraycopy(bos.getInternalBuffer(), readPos, remaining, 0, remainingSize);
                bos.reset();
                bos.write(remaining, 0, remainingSize);
                readPos = 0;
            }

            // get more source data
            if (source.writeNext()) {
                return read(dst, off, len);
            } else {
                // no more source data
                int remainingSize = bos.size() - readPos;
                if (remainingSize == 0) {
                    return -1;
                } else {
                    len = Math.min(len, remainingSize);
                }
            }
        }

        // write buf and adjust read position
        System.arraycopy(bos.getInternalBuffer(), readPos, dst, off, len);
        readPos += len;
        return len;
    }
}

