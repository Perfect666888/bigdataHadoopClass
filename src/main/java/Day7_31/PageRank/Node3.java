package Day7_31.PageRank;

import org.apache.commons.lang.StringUtils;

import java.util.Arrays;

public class Node3 {
    //切割标识
    private static final String splitRegex = "\t";

    //存储pr值对象
    private double pageRank = 1.0;

    //存储指向信息
    private String[] informations;

    public Node3() {
        super();
    }

    public double getPageRank() {
        return pageRank;
    }

    public void setPageRank(double pageRank) {
        this.pageRank = pageRank;
    }

    public String[] getInformations() {
        return informations;
    }

    public void setInformations(String[] informations) {
        this.informations = informations;
    }

    //创建对象方法
    public static Node3 setNode3(String line) {
        Node3 node3 = new Node3();
        //按照指定规则切割字符串
        String[] strings = StringUtils.splitPreserveAllTokens(line, splitRegex);
        //第一位为值
        node3.setPageRank(Double.parseDouble(strings[0]));
        //后面为指向信息，可能为null，所以需要判断
        if (strings.length > 1) {
            node3.setInformations(Arrays.copyOfRange(strings, 1, strings.length));
        }
        return node3;
    }

    //判断指向信息是否为null
    public boolean pdInformations() {

        return informations != null && informations.length > 0;
        //2个判断的原因
        /*
        *         String[] str =new String[0];
        System.out.println(str.length);
        for (String s : str) {
            System.out.println(s);
        }


        System.out.println("========================");
        String[] str2 =null;
        System.out.println(str2.length);
        for (String s : str2) {
            System.out.println(s);
        }
        * */
    }

    @Override
    public String toString() {
        //为了线程安全
        StringBuffer sb = new StringBuffer();
        sb.append(pageRank);
        //不为null,才添加指向信息
        if (getInformations()!=null) {
            //间隔符
            sb.append(splitRegex).append(StringUtils.join(informations, splitRegex));
        }
        return sb.toString();
    }


}
