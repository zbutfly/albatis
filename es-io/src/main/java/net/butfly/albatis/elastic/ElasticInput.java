package net.butfly.albatis.elastic;

import net.butfly.albacore.io.Input;
import net.butfly.albacore.utils.logger.Logger;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsRequest;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilders;

import java.io.IOException;
import java.util.List;

public class ElasticInput extends Input<SearchResponse> {
    private static final long serialVersionUID = -5666669099160512388L;
    protected static final Logger logger = Logger.getLogger(ElasticInput.class);
    private final ElasticConnection elastic;
    //view time def 10 minute
    private int scrolltime = 10;
    //scrollnumber
    private int scrollnumber;
    private SearchResponse searchResponse = null;

    public ElasticInput(String connection) throws IOException {
        super("elastic-input-queue");
        elastic = new ElasticConnection(connection);
    }

    @Override
    public void closing() {
        super.closing();
        try {
            elastic.close();
        } catch (IOException e) {
            logger.error("Close failure", e);
        }
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
    public SearchResponse dequeue0() {
        if (searchResponse == null)
            scanType();
        else scanType2(searchResponse);
        return searchResponse;
    }

    @Override
    public List<SearchResponse> dequeue(long batchSize) {
        return super.dequeue(batchSize);

    }

    @SuppressWarnings("deprecation")
    public void scanType() {
        SearchRequestBuilder searchRequest = elastic.client().prepareSearch("scattered_data").setSize(scrollnumber)
                .setSearchType(SearchType.SCAN).setScroll(TimeValue.timeValueMinutes(scrolltime))
                .setQuery(QueryBuilders.matchAllQuery());
        logger.trace(searchRequest.toString());
        searchResponse = searchRequest.execute().actionGet();
    }

    private SearchResponse scanType2(SearchResponse searchResponse) {
        SearchResponse searchResponse2 = elastic.client().prepareSearchScroll(searchResponse.getScrollId())
                .setScroll(TimeValue.timeValueMinutes(scrolltime)).execute().actionGet();
        return searchResponse2;
    }
}
