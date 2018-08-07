package Day7_31.PageRank;

import org.apache.commons.lang.StringUtils;

import java.util.Arrays;

public class Node2 {

    //创建变量存储PR,初始化为1.0
    private double rankScore = 1.0;

    //创建字符串数组，存放值以后的信息（）
    //默认初始化即可，1，长度不一定，2可能为null
    private String[] informations;

    //创建切割规则，并且无法修改
    private static final String splitRegex = "\t";

    //无参构造，创建对象
    public Node2() {
        super();
    }

    //get，set方法
    public double getRankScore() {
        return rankScore;
    }

    public void setRankScore(double rankScore) {
        this.rankScore = rankScore;
    }

    public String[] getInformations() {
        return informations;
    }

    public void setInformations(String[] informations) {
        this.informations = informations;
    }

    //有参,创建对象的方法
    public static Node2 setNode(String line) {
        //创建对象
        Node2 node2 = new Node2();
        //传进来的数据按照指定规则切割
        String[] strings = StringUtils.splitPreserveAllTokens(line, splitRegex);
        //等价于
        //String[] splits = line.split(splitRegex);

        //第一字符串为PR值，直接赋值
        node2.setRankScore(Double.parseDouble(strings[0]));

        //因为需要截取pr值后面的内容存储,所以需要先判断数组长度是否大于1
        if (strings.length > 1) {
            //直接完成赋值
            node2.setInformations(Arrays.copyOfRange(strings, 1, strings.length));
        }//不大于1，会完成默认初始化，即位null
        //返回
        return node2;

    }

    //判断是否informations是否为空
    public boolean pdInformations() {
        return informations != null && informations.length > 0;
    }

    //重写tostring,避免当informations为null时，输出不无用数据
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(rankScore);
        //当informations不为null时，才输出
        if(getInformations()!=null){
            //用切割规则作为分隔符
            sb.append(splitRegex).append(StringUtils.join(informations,splitRegex));
        }

        return sb.toString();
    }
}
