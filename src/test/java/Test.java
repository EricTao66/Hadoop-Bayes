import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import prediction.Util;

import java.net.URI;

public class Test {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        FileSystem hdfs = FileSystem.get(URI.create(Util.INPUT_PATH), conf);
        FileStatus[] fs = hdfs.listStatus(new Path(Util.INPUT_PATH));
        Path[] listPath = FileUtil.stat2Paths(fs);
        for (Path p : listPath) {
            System.out.println(p);
        }
    }
}
