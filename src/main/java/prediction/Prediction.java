package prediction;

import org.apache.hadoop.fs.*;

import java.io.*;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Map;

public class Prediction {

    private static Hashtable<String, Double> class_prob = new Hashtable<String, Double>();
    private static Hashtable<Map<String, String>, Double> class_term_prob = new Hashtable<Map<String, String>, Double>();
    private static Hashtable<String, Double> class_term_total = new Hashtable<String, Double>();
    private static Hashtable<String, Double> class_term_num = new Hashtable<String, Double>();

    Prediction(FileSystem fs) throws IOException {
        // 统计文档总数
        BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(
                new Path(Util.OUTPUT_PATH + "docCount/part-r-00000"))));
        double file_total = 0;
        while (reader.ready()) {
            String line = reader.readLine();
            String[] args = line.split("\t");// 0：类，1：数量
            file_total += Double.parseDouble(args[1]);
        }

        // 计算先验概率class_prob
        reader = new BufferedReader(new InputStreamReader(fs.open(
                new Path(Util.OUTPUT_PATH + "docCount/part-r-00000"))));
        while (reader.ready()) {
            String line = reader.readLine();
            String[] args = line.split("\t");// 0：类，1：数量
            class_prob.put(args[0], Double.parseDouble(args[1]) / file_total);
            System.out.println(String.format("%s:%f", args[0], Double.parseDouble(args[1]) / file_total));
        }

        //计算单词总数和单词集合大小
        reader = new BufferedReader(new InputStreamReader(fs.open(
                new Path(Util.OUTPUT_PATH + "wordCount/part-r-00000"))));
        while (reader.ready()) {
            String line = reader.readLine();
            String[] args = line.split("\t");// 0：类，1：词条，2：词频
            double count = Double.parseDouble(args[2]);
            String classname = args[0];
            class_term_total.put(classname, class_term_total.getOrDefault(classname, 0.0) + count);
            class_term_num.put(classname, class_term_num.getOrDefault(classname, 0.0) + 1.0);
        }

        /*
        //计算单词集合大小
        reader = new BufferedReader(new InputStreamReader(fs.open(
                new Path(Util.OUTPUT_PATH + "wordCount/part-r-00000"))));
        while (reader.ready()) {
            String line = reader.readLine();
            String[] args = line.split("\t");// 0：类，1：词条，2：词频
            String classname = args[0];
            class_term_num.put(classname, class_term_num.getOrDefault(classname, 0.0) + 1.0);
        }
        */

        //计算每个类别里面出现的词条概率class-term prob
        reader = new BufferedReader(new InputStreamReader(fs.open(
                new Path(Util.OUTPUT_PATH + "wordCount/part-r-00000"))));
        while (reader.ready()) {
            String line = reader.readLine();
            String[] args = line.split("\t");// 0：类，1：词条，2：词频
            double count = Double.parseDouble(args[2]);
            String classname = args[0];
            String term = args[1];
            Map<String, String> map = new HashMap<String, String>();
            map.put(classname, term);
            class_term_prob.put(map, (count + 1) / (class_term_total.get(classname) + class_term_num.get(classname)));
        }
    }

    // 计算一个文档属于某类的条件概率
    public static double probForClass(String content, String classname) {
        double result = 0;
        String[] words = content.split("\n");
        for (String word : words) {
            Map<String, String> map = new HashMap<String, String>();
            map.put(classname, word);
            result += Math.log(class_term_prob.getOrDefault(map, 1.0 / (class_term_total.get(classname) + class_term_num.get(classname))));
        }
        result += Math.abs(Math.log(class_prob.get(classname)));
        return result;
    }
}