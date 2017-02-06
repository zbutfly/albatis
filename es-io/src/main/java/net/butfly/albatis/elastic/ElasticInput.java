package net.butfly.albatis.elastic;

import java.io.IOException;

import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilders;

import net.butfly.albacore.io.InputImpl;
import net.butfly.albacore.utils.logger.Logger;

public final class ElasticInput extends InputImpl<SearchResponse> {
	protected static final Logger logger = Logger.getLogger(ElasticInput.class);
	private final ElasticConnection elastic;
	// view time def 10 minute
	private int scrolltime = 10;
	// scrollnumber
	private int scrollnumber = 100;
	private SearchResponse searchResponse = null;
	private String index;
	private String type;

	public ElasticInput(String connection) throws IOException {
		elastic = new ElasticConnection(connection);
		index = elastic.getDefaultIndex();
		type = elastic.getDefaultType();
		open();
	}

	@Override
	public void close() {
		super.close(elastic::close);
	}

	public int getScrolltime() {
		return scrolltime;
	}

	public void setScrolltime(int scrolltime) {
		this.scrolltime = scrolltime;
	}

	public int getScrollnumber() {
		return scrollnumber;
	}

	public void setScrollnumber(int scrollnumber) {
		this.scrollnumber = scrollnumber;
	}

	public String getIndex() {
		return index;
	}

	public void setIndex(String index) {
		this.index = index;
	}

	public String getType() {
		return type;
	}

	public void setType(String type) {
		this.type = type;
	}

	@Override
	public long size() {
		return 0;
	}

	@Override
	public boolean empty() {
		return true;
	}

	@Override
	public SearchResponse dequeue() {
		if (searchResponse == null) scanType();
		else scanType2(searchResponse);
		return searchResponse;
	}

	public void scanType() {
		if (index == null) throw new RuntimeException("index is null");
		SearchRequestBuilder searchRequest = elastic.client().prepareSearch(index);
		searchRequest.setSize(scrollnumber)
				// .setSearchType(SearchType.SCAN)
				.setScroll(TimeValue.timeValueMinutes(scrolltime)).setQuery(QueryBuilders.matchAllQuery());
		logger.trace(searchRequest.toString());
		searchResponse = searchRequest.execute().actionGet();
	}

	private SearchResponse scanType2(SearchResponse searchResponse) {
		return elastic.client().prepareSearchScroll(searchResponse.getScrollId()).setScroll(TimeValue.timeValueMinutes(scrolltime))
				.execute().actionGet();
	}
}
