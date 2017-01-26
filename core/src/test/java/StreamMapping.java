import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import scala.Tuple2;

public class StreamMapping {

	public static void main(String[] args) {
		List<Tuple2<String, String>> origin = new ArrayList<>();
		origin.add(new Tuple2<>("a", "a1"));
		origin.add(new Tuple2<>("a", "a2"));
		origin.add(new Tuple2<>("a", "a3"));
		origin.add(new Tuple2<>("a", "a4"));
		origin.add(new Tuple2<>("a", "a5"));
		origin.add(new Tuple2<>("b", "b1"));
		origin.add(new Tuple2<>("b", "b2"));
		origin.add(new Tuple2<>("b", "b3"));
		Map<String, List<String>> map = origin.stream().collect(Collectors.groupingBy(t -> t._1, Collectors.mapping(t -> t._2, Collectors
				.toList())));
		System.err.println(map);
	}

}
