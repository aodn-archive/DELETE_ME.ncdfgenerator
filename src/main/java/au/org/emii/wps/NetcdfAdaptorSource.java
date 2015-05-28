package au.org.emii.wps;

import java.io.OutputStream;
import java.io.IOException;

import java.sql.Connection;
import java.sql.SQLException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.geotools.data.Transaction;

import au.org.emii.ncdfgenerator.NcdfEncoder;
import au.org.emii.ncdfgenerator.ZipFormatter; 


class NetcdfAdaptorSource implements StreamAdaptorSource
{
    private static final Logger logger = LoggerFactory.getLogger(NetcdfAdaptorSource.class);
    private final NcdfEncoder encoder;
    private final Transaction transaction; 
    private final Connection conn;

    NetcdfAdaptorSource(NcdfEncoder encoder, Transaction transaction, Connection conn) {   
        this.encoder = encoder;
        this.transaction = transaction; 
        this.conn = conn;
    }

    public void prepare(OutputStream os) throws Exception {   
        encoder.prepare(new ZipFormatter(os));
    }

    public boolean writeNext() throws IOException {
        try { 
            return encoder.writeNext();
        } catch (Exception e) { 
            close();
            throw new IOException(e);
        }
    }

    public void close() {
        try {
            transaction.close();
        } catch (IOException e) {
            logger.info("problem closing transaction");
        }
        try {
            conn.close();
        } catch (SQLException e) {
            logger.info("problem closing connection");
        }
    }
}

