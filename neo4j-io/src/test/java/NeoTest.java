

import org.neo4j.driver.v1.AuthToken;
import org.neo4j.driver.v1.AuthTokens;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.GraphDatabase;
import org.neo4j.driver.v1.Record;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.StatementResult;

import net.butfly.albacore.utils.collection.Maps;

public class NeoTest {
	public static void main(String[] args) {
		AuthToken auth = AuthTokens.basic("neo4j", "neo4j");
		try (Driver d = GraphDatabase.driver("bolt://localhost:7687", auth); Session s = d.session();) {
			s.run("CREATE (a:Person {name: {name}, title: {title}})", Maps.of("name", "Arthur", "title", "King"));
			StatementResult r = s.run("MATCH (a:Person) WHERE a.name = {name} " + "RETURN a.name AS name, a.title AS title", //
					Maps.of("name", "Arthur"));
			while (r.hasNext()) {
				Record record = r.next();
				System.out.println(record.get("title").asString() + " " + record.get("name").asString());
			}
		}
	}
}
