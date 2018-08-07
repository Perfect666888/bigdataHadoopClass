package Day8_03;

import java.sql.*;

/**
 * @author Perfect
 * @date 2018/8/5 14:15
 */
public class hiveJdbc {
    public static void main(String[] args) throws ClassNotFoundException, SQLException {
        //加载驱动
        Class.forName("org.apache.hive.jdbc.HiveDriver");
        //建立连接
        Connection con = DriverManager.getConnection("jdbc:hive2://192.168.41.200:10000/hive");
        Statement st = con.createStatement();

        //建立查询语句
        ResultSet resultSet = st.executeQuery("select * from student.score limit 10");

        while (resultSet.next()) {
            System.out.println(resultSet.getString(1) + "----" + resultSet.getString(2) + "-----" + resultSet.getInt(3));
        }

        //释放资源
        resultSet.close();
        st.close();
        con.close();
    }
}
