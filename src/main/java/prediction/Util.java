package path;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.net.URI;

public class Util {

    public static String[] CLASS_NAMES={"BRAZ","CANA"};//训练及测试选取的类别
    public static String INPUT_PATH="src/main/resources/input/";//输入目录
    public static String OUTPUT_PATH="src/main/resources/output/";//输出目录

    //递归删除目录下的所有文件及子目录下所有文件
    public static boolean deleteDir(String path) {
        File dir = new File(path);
        if (!dir.exists()) {
            return false;
        }
        if (dir.isDirectory()) {
            File[] files = dir.listFiles();
            for (File f:files) {
                deleteDir(f.getPath());
            }
        }
        return dir.delete();
    }
    public static ArrayList<Path> getPaths(String path) {
        // 获取path路径下所有子文件夹路径
        ArrayList<Path> paths = new ArrayList<Path>();
        File file = new File(path);
        if (file.isDirectory()) {// 如果这个路径是文件夹
            File[] files = file.listFiles();// 获取路径下的所有文件
            for (File f:files) {
                if (file.isDirectory()) {// 如果还是文件夹
                    paths.add(new Path(f.getPath()));// 将其加入路径列表
                }
                else {continue;}
            }
        }
        return paths;
    }
}
