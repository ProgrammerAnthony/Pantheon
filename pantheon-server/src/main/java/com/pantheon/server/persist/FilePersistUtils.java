package com.pantheon.server.persist;

import com.pantheon.server.config.ArchaiusPantheonServerConfig;
import com.pantheon.server.config.CachedPantheonServerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.util.zip.Adler32;
import java.util.zip.Checksum;

public class FilePersistUtils {

    private static final Logger LOGGER = LoggerFactory.getLogger(FilePersistUtils.class);


    /**
     * save to disk
     * @param bytes
     * @param filename
     * @return
     */
    public static Boolean persist(byte[] bytes, String filename) {
        try {
            // get data saving directory
            File dataDir = new File(CachedPantheonServerConfig.getInstance().getDataDir());
            if(!dataDir.exists()) {
                dataDir.mkdirs();
            }

            File file = new File(dataDir, filename);
            FileOutputStream fileOutputStream = new FileOutputStream(file);
            BufferedOutputStream bufferedOutputStream = new BufferedOutputStream(fileOutputStream);
            DataOutputStream dataOutputStream = new DataOutputStream(bufferedOutputStream);

            //checksum for data in disk
            Checksum checksum = new Adler32();
            checksum.update(bytes, 0, bytes.length);
            long checksumValue = checksum.getValue();
            dataOutputStream.writeLong(checksumValue);
            dataOutputStream.writeInt(bytes.length);
            dataOutputStream.write(bytes);
            //flush to fileOutputStream
            bufferedOutputStream.flush();
            // flush to os cache
            fileOutputStream.flush();
            // flush to disk
            fileOutputStream.getChannel().force(false);
        } catch(Exception e) {
            LOGGER.error("persist file error......", e);
            return false;
        }
        return true;
    }

}
