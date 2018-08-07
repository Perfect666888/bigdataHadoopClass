package Day7_31.PageRank;


import org.apache.commons.lang.StringUtils;

import java.util.Arrays;

public class Node {

    //创建rank变量存储值
    private double rankScore =1.0;

    //创建string数组存储信息
    private String[] informations;

    //指定切割标识，并且不能修改
    private static final char splitRegex= '\t';

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

    @Override
    public String toString() {
        //字符串缓冲区
        StringBuilder sb = new StringBuilder();
        //先把rankscore添加到sb
        sb.append(rankScore);
        //informations可能为null，所以要先判断
        if(getInformations()!=null){
            //不为空时，输出
            sb.append(splitRegex).append(StringUtils.join(getInformations(),splitRegex));
        }

        //返回结果
        return  sb.toString();
    }

    //判断信息变量是否为空
    public  boolean pdInformations(){
        return informations !=null && informations.length>0;
    }


    //赋值方法
    public static Node setNode(String line){
        Node node = new Node();
        //输入进来的数据进行切割
        //获得值和信息
        String[] strs = StringUtils.splitPreserveAllTokens(line, splitRegex);

        //获得值,并赋值
        node.setRankScore(Double.parseDouble(strs[0]));
        //判断数组长度是不是大于1,
        //大于1才能对于informations赋值
        if(strs.length>1){
            //截取数组,赋值
            node.setInformations(Arrays.copyOfRange(strs,1,strs.length));
        }

        return node;
    }



}
