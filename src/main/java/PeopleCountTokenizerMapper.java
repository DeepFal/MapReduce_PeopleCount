import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

//统计人数,继承Mapper类,设置输入类型<Object, Text>和输出类型<Text, IntWritable>
public class PeopleCountTokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {
    private static final IntWritable one = new IntWritable(1);
    private final Text author = new Text();
    private Text survival = new Text();
    private Text die = new Text();
    private Text sex = new Text();

    public PeopleCountTokenizerMapper() {
    }

    public void map(Object key, Text value, Mapper<Object, Text, Text, IntWritable>.Context context) throws IOException, InterruptedException {
        //使用逗号分隔符分割字符串
        StringTokenizer itr = new StringTokenizer(value.toString(), ",");
        //输出作者信息
        this.author.set("Author:\nChenYuwen\nLiuShun\nHeZhiwei\n2023-4-19\n总计算次数:");
        context.write(this.author, one);
        //循环遍历分割后的字符串,hasMoreTokens()判断是否还有分割后的字符串
        while (itr.hasMoreTokens()) {
            //获取每个分割后的字符串,trim()去除字符串两端的空格
            String token = itr.nextToken().trim();
            //将字符串分割为数组
            String[] values = value.toString().split(",");
            //判断是否存活，equalsIgnoreCase()忽略大小写
            if (token.equalsIgnoreCase("Yes")) {
                this.survival.set("存活");
                context.write(this.survival, one);
                //判断性别，values[4]为性别字段
                if (values[4].equalsIgnoreCase("Male")) {
                    this.sex.set("男性存活数");
                    context.write(this.sex, one);
                } else if (values[4].equalsIgnoreCase("Female")) {
                    this.sex.set("女性存活数");
                    context.write(this.sex, one);
                }
                //判断是否死亡，equalsIgnoreCase()忽略大小写
            } else if (token.equalsIgnoreCase("No")) {
                this.die.set("死亡");
                context.write(this.die, one);
                //判断性别，values[4]为性别字段
                if (values[4].equalsIgnoreCase("Male")) {
                    this.sex.set("男性死亡数");
                    context.write(this.sex, one);
                } else if (values[4].equalsIgnoreCase("Female")) {
                    this.sex.set("女性死亡数");
                    context.write(this.sex, one);
                }
            }
            //判断性别
            if (token.equalsIgnoreCase("Male")) {
                this.sex.set("男性总数");
                context.write(this.sex, one);
            } else if (token.equalsIgnoreCase("Female")) {
                this.sex.set("女性总数");
                context.write(this.sex, one);
            }
        }
    }
}
