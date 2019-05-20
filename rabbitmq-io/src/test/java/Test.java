import net.butfly.albacore.io.URISpec;

public class Test {
	
	public static void main(String[] args) {
		String url = "datahub://YourAccessKeyId:YourAccessKeySecret@dh-cn-hangzhou.aliyuncs.com/YourProjectName";
		URISpec ui = new URISpec(url);
		System.out.println(ui.getUsername());
		System.out.println(ui.getPassword());
		System.out.println(ui.getFile());
		System.out.println(ui.getHost());
	}
}
