package net.butfly.albatis.bcp.utils;

/**
 * nothing.
 *
 * @author : kqlu
 * @version :
 * @code :
 * @since : Created in 15:24 2019/3/1
 */
public class Format {
	//所有的函数都只能有一个string类型的参数
	public static String idnoTrans15To18(String idno){
		return idno;
	}

	public static String baseMix(String str){
		if (str == null || str.equals("")){
			return "*";
		} else {
			StringBuilder sb = new StringBuilder();
			for (int i = 0; i < str.length(); i++) {
				sb.append("*");
			}
			return sb.toString();
		}
	}
}
