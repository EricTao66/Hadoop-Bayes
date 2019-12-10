package prediction;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

public class Util {

    public static final String[] CLASS_NAMES = {"BRAZ", "CANA"};//训练及测试选取的类别

//    public static final String BASE_PATH = "src/main/resources/";//单机模式
//    public static final String BASE_PATH = "hdfs://localhost:9000/user/erictao/";//伪分布式
    public static final String BASE_PATH = "hdfs://master:9000/";//分布式

    public static final String INPUT_PATH = BASE_PATH + "input/";//输入目录
    public static final String OUTPUT_PATH = BASE_PATH + "output/";//输出目录

    // 获取path路径下所有子文件夹路径
    public static Path[] getPaths(Path path, FileSystem fs)
            throws IOException {
        FileStatus[] stat = fs.listStatus(path);
        Path[] listPath = FileUtil.stat2Paths(stat);
        return listPath;
    }
}
