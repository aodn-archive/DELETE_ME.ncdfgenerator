package au.org.emii.ncdfgenerator;

import java.io.InputStream;
import java.io.OutputStream;

public interface IOutputFormatter {

    // should we be returning bool ? 
    void write(String filename, InputStream is) throws Exception;
    void close() throws Exception; // why do we need this... to be able to close everything down...
}

