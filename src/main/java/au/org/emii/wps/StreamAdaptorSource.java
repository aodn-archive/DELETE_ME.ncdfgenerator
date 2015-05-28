package au.org.emii.wps;

import java.io.OutputStream;
import java.io.IOException;

interface StreamAdaptorSource
{
    void prepare(OutputStream os) throws Exception;
    boolean writeNext() throws IOException;
    void close();
}

