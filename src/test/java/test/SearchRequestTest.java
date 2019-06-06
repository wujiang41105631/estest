package test;

import org.elasticsearch.action.search.*;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.profile.ProfileShardResult;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.junit.Test;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * @author: xupeng.guo
 * @date: 2019/6/5
 * @description
 */
public class SearchRequestTest extends ElasticSearchTest {

    /**
     * queryBuilder https://www.cnblogs.com/sunfie/p/9030196.html
     * @throws IOException
     */
    @Test
    public void testSearchRequest() throws IOException {
        SearchRequest searchRequest = new SearchRequest("asset_info1");
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.matchAllQuery());
        searchSourceBuilder.from(0);
        searchSourceBuilder.size(5);
        searchSourceBuilder.timeout(new TimeValue(60, TimeUnit.SECONDS));
        // 排序
        searchSourceBuilder.sort(new FieldSortBuilder("id").order(SortOrder.ASC));// text 不让排序,keyward 可以排序

        // 聚合
//        TermsAggregationBuilder aggregation = AggregationBuilders.terms("by_company")
//                .field("company.keyword");
//        aggregation.subAggregation(AggregationBuilders.avg("average_age")
//                .field("age"));
//        searchSourceBuilder.aggregation(aggregation);


//        String[] includeFields = new String[] {"title", "innerObject.*"};
//        String[] excludeFields = new String[] {"user"};
//        searchSourceBuilder.fetchSource(includeFields, excludeFields);
        /**
         * 高亮
         */
//        HighlightBuilder highlightBuilder = new HighlightBuilder();
//        HighlightBuilder.Field highlightTitle =
//                new HighlightBuilder.Field("title");
//        highlightTitle.highlighterType("unified");
//        highlightBuilder.field(highlightTitle);
//        HighlightBuilder.Field highlightUser = new HighlightBuilder.Field("user");
//        highlightBuilder.field(highlightUser);
//        searchSourceBuilder.highlighter(highlightBuilder);


        searchRequest.indicesOptions(IndicesOptions.lenientExpandOpen());//设置IndicesOptions控制如何解决不可用的索引以及如何扩展通配符表达式
        searchRequest.source(searchSourceBuilder);
//        searchSourceBuilder.profile(true);
        SearchResponse response = client.search(searchRequest, RequestOptions.DEFAULT);
//        System.out.println(response.status());
//        System.out.println(response.getTook());
//        System.out.println(response.isTerminatedEarly());
//        System.out.println(response.isTimedOut());
//        System.out.println(response.getTotalShards());
//        System.out.println(response.getSuccessfulShards());
//        System.out.println(response.getFailedShards());
//        System.out.println(response.getHits().getTotalHits().value);
        SearchHit[] hits = response.getHits().getHits();
        for (SearchHit hit : hits) {
            System.out.println(hit.getSourceAsString());
        }
//        ShardSearchFailure[] shardFailures = search.getShardFailures();
//        for (ShardSearchFailure shardFailure : shardFailures) {
//            System.out.println(shardFailure.reason());
//        }
            //分析结果
//        Map<String, ProfileShardResult> profileResults = response.getProfileResults();
//        System.out.println(profileResults.size());
//        profileResults.forEach((key, value) -> {
//            System.out.println(key + ":" + value.getQueryProfileResults().get(0));
//        });
    }

    @Test
    public void testMutiSearchRequest() throws IOException {
        MultiSearchRequest request = new MultiSearchRequest();
        SearchRequest firstSearchRequest = new SearchRequest("asset_info");
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.matchQuery("message", "out"));
        firstSearchRequest.source(searchSourceBuilder);
        request.add(firstSearchRequest);
        SearchRequest secondSearchRequest = new SearchRequest("asset_info1");
        searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.matchQuery("userName", "tomcat"));
        secondSearchRequest.source(searchSourceBuilder);
        request.add(secondSearchRequest);
        MultiSearchResponse response = client.msearch(request, RequestOptions.DEFAULT);
        MultiSearchResponse.Item[] items = response.getResponses();
        for (MultiSearchResponse.Item item : items) {
            SearchHit[] hits = item.getResponse().getHits().getHits();
            for (SearchHit hit : hits) {
                System.out.println(hit.getSourceAsString()+ "--"+ hit.getIndex());
            }

        }
    }
}
