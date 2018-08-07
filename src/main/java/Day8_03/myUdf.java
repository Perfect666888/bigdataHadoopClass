package Day8_03;


import org.apache.hadoop.hive.ql.exec.UDF;

/**
 * @author Perfect
 * @date 2018/8/3 21:10
 */
public class myUdf  extends UDF {
    public int evaluate(int mark){
        int newMark=0;
        if (mark > 60){
            newMark=100;
        }
        return newMark;
    }



}
