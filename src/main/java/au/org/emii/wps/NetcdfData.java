package au.org.emii.wps;

import java.io.InputStream;

import org.geoserver.wps.process.RawData;

class NetcdfData implements RawData
{
    private final InputStream is;

    NetcdfData(InputStream is) {
        this.is = is;
    }

    public InputStream getInputStream() {
        return is;
    }

    public String getMimeType() {
        return "application/zip";
    }

    public String getFileExtension() {
        return "zip";
    }
}

