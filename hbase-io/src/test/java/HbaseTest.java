
public class HbaseTest {
	public static void main(String[] args) {
		System.out.println(batch(1, 1000));
		System.out.println(batch(998, 1000));
		System.out.println(batch(999, 1000));
		System.out.println(batch(1000, 1000));
		System.out.println(batch(1001, 1000));
	}

	private static int batch(int total, int batch) {
		// return (total - 1) / batch + 1;
		return (total + 1) / batch;
	}
}
