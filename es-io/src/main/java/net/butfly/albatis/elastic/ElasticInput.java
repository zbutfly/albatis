package net.butfly.albatis.elastic;

import net.butfly.albacore.io.Input;
import net.butfly.albacore.utils.logger.Logger;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
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
    private int scrollnumber = 100;
    private SearchResponse searchResponse = null;
    private String index;
    private String type;

    public ElasticInput(String connection) throws IOException {
        super("elastic-input-queue");
        elastic = new ElasticConnection(connection);
        index = elastic.getDefaultIndex();
        type = elastic.getDefaultType();
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
        if (index == null)
            throw new RuntimeException("index is null");
        SearchRequestBuilder searchRequest = elastic.client()
                .prepareSearch(index);
        searchRequest.setSize(scrollnumber)
              //  .setSearchType(SearchType.SCAN)
                .setScroll(TimeValue.timeValueMinutes(scrolltime))
                .setQuery(QueryBuilders.matchAllQuery());
        logger.trace(searchRequest.toString());
        searchResponse = searchRequest.execute().actionGet();
    }

    private SearchResponse scanType2(SearchResponse searchResponse) {
        return elastic.client().prepareSearchScroll(searchResponse.getScrollId())
                .setScroll(TimeValue.timeValueMinutes(scrolltime)).execute().actionGet();
    }
}
