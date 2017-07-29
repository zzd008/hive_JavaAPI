package hive_jdbc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

//jdbc连接hive javaweb中连接mysql需要自己导入jar包，而这里所需要的驱动jar包在hive/lib下,（如hive-jdbc-1.2.1.jar hive连接mysql获取元数据所用的mysql-connector-java5.1.2-等等）可能也需要hdfs的jar包
//hive1.0版本后使用hiveserver服务，所以有些代码和hiveserver（1.0版本之前）有些不同
/**
 * org.apache.hadoop.hive.jdbc.HiveDriver 改为：org.apache.hive.jdbc.HiveDriver
 * jdbc:hive://localhost:10000/default 改为：jdbc:hive2://localhost:10000/default
 */
public class HiveTest {
	private static String driverName="org.apache.hive.jdbc.HiveDriver";
	public static void main(String[] args) throws SQLException {
		try {
			Class.forName(driverName);
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
			System.exit(1);
		}
		
		//Connection con=DriverManager.getConnection("jdbc:hive2://localhost:10000/default"); 连接hive的密码默认为空，写不写，乱写都行。可以在hive-site.xml中配置 参考http://blog.csdn.net/lr131425/article/details/72628001
		Connection con=DriverManager.getConnection("jdbc:hive2://localhost:10000/default","","");
		Statement st=con.createStatement();
		
		//创建表
		String tableName="testtable";
		String sql1="drop table if exists "+tableName;
		String sql2="create table "+tableName+" (key int,value string)";
		System.out.println("执行"+sql1);
		st.execute(sql1);
		System.out.println("执行"+sql2);
		st.execute(sql2);
		System.out.println("表testtable创建成功！");
		
		//显示表
		String sql3="show tables "+tableName;
		System.out.println("执行"+sql3);
		ResultSet rs = st.executeQuery(sql3);
		if(rs.next()){
			System.out.println(rs.getString(1));
		}
	
		//描述表
		String sql4="describe "+tableName;
		System.out.println("执行"+sql4);
		rs=st.executeQuery(sql4);
		while(rs.next()){
			System.out.println(rs.getString(1)+"\t"+rs.getString(2));
		}
		sql4="select *from "+tableName;
		System.out.println("执行"+sql4);
		rs = st.executeQuery(sql4);
		while(rs.next()){
			System.out.println(String.valueOf(rs.getInt(1)+"\t"+rs.getString(2)));
		}
		
	}
}
