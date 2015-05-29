package au.org.emii.wps;

import java.io.InputStream;

import org.geoserver.wps.process.StreamRawData;

class NetcdfData extends StreamRawData
{
    NetcdfData(InputStream inputStream) {
        super("application/zip", inputStream, "zip");
    }
}
