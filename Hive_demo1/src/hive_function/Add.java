package hive_function;

import org.apache.hadoop.hive.ql.exec.UDF;

//用户自定义hive函数
public class Add extends UDF{
	public Integer evaluate(Integer a,Integer b){
		if(a!=null&&b!=null){
			return a+b;
		}
		return null;
	}
}
