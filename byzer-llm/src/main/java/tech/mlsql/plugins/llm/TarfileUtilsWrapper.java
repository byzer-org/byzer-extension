package tech.mlsql.plugins.llm;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.kamranzafar.jtar.TarOutputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import streaming.core.HDFSTarEntry;
import tech.mlsql.tool.HDFSOperatorV2;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

/**
 * 4/4/23 WilliamZhu(allwefantasy@gmail.com)
 */
public class TarfileUtilsWrapper {
    private static Logger logger = LoggerFactory.getLogger(TarfileUtilsWrapper.class);

    public static void walk(FileSystem fs, List<FileStatus> files, Path p, FileNameFilter fileNameFilter) throws IOException {

        if (fs.isFile(p) && fileNameFilter.accept(p.getName())) {
            files.add(fs.getFileStatus(p));
        } else if (fs.isDirectory(p) && fileNameFilter.accept(p.getName())) {
            FileStatus[] fileStatusArr = fs.listStatus(p);
            if (fileStatusArr != null && fileStatusArr.length > 0) {
                for (FileStatus cur : fileStatusArr) {
                    walk(fs, files, cur.getPath(), fileNameFilter);
                }
            }

        }
    }

    public static int createTarFileStream(OutputStream output, String pathStr, FileNameFilter fileNameFilter) throws IOException {
        FileSystem fs = FileSystem.get(HDFSOperatorV2.hadoopConfiguration());
        String[] paths = pathStr.split(",");
        try {
            OutputStream outputStream = output;

            TarOutputStream tarOutputStream = new TarOutputStream(new BufferedOutputStream(outputStream));

            List<FileStatus> files = new ArrayList<FileStatus>();

            for (String path : paths) {
                logger.info("Walk the path: " + path);
                TarfileUtilsWrapper.walk(fs, files, new Path(path), fileNameFilter);
            }

            if (files.size() > 0) {
                FSDataInputStream inputStream = null;
                int len = files.size();
                int i = 1;
                for (FileStatus cur : files) {
                    inputStream = fs.open(cur.getPath());
                    URI tempUri = cur.getPath().toUri();
                    String prefix = "";
                    if(tempUri.getScheme()!=null){
                        prefix = tempUri.getScheme()+"://";
                    }
                    prefix = prefix + tempUri.getPath();
                    String entryName = StringUtils.removeStart(prefix, pathStr);
                    logger.info("[" + i++ + "/" + len + "]" + entryName + ",读取文件" + cur);
                    tarOutputStream.putNextEntry(new HDFSTarEntry(cur, entryName));
                    org.apache.commons.io.IOUtils.copyLarge(inputStream, tarOutputStream);
                    inputStream.close();

                }
                tarOutputStream.flush();
                tarOutputStream.close();
                return 200;
            } else return 400;

        } catch (Exception e) {
            e.printStackTrace();
            return 500;

        }
    }
}
