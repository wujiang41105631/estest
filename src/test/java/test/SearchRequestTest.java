package test;

import org.elasticsearch.action.search.MultiSearchRequest;
import org.elasticsearch.action.search.MultiSearchResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.core.CountRequest;
import org.elasticsearch.client.core.CountResponse;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.BucketOrder;
import org.elasticsearch.search.aggregations.PipelineAggregatorBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.*;
import org.elasticsearch.search.aggregations.pipeline.StatsBucket;
import org.elasticsearch.search.aggregations.pipeline.StatsBucketPipelineAggregationBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

/**
 * @author: xupeng.guo
 * @date: 2019/6/5
 * @description
 */
public class SearchRequestTest extends ElasticSearchTest {

    /**
     * queryBuilder https://www.cnblogs.com/sunfie/p/9030196.html
     *
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

    //    @Test
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
                System.out.println(hit.getSourceAsString() + "--" + hit.getIndex());
            }

        }
    }

    @Test
    public void testCountRequest() throws IOException {
        CountRequest countRequest = new CountRequest("temp_asset_info");
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.termQuery("card_no", "65dk3bdkPg+r2qmmd7LdwIMdzPpOkYZqTv3l1Tv8/n4="));
        countRequest.source(searchSourceBuilder);
        CountResponse response = client.count(countRequest, RequestOptions.DEFAULT);
        if (response.status() == RestStatus.OK) {
            logger.info("count : {}", response.getCount());
        }
    }

    @Test
    public void testGroupCount() throws IOException {
//        ValueCountAggregationBuilder field1 = AggregationBuilders.count("gradeAgg").field("card_no");
//        SearchSourceBuilder searchSourceBuilder1= new SearchSourceBuilder();
//        searchSourceBuilder1.aggregation(field1);
//        CountRequest countRequest = new CountRequest("temp_asset_info");
//        countRequest.source(searchSourceBuilder1);
//        CountResponse r = client.count(countRequest, RequestOptions.DEFAULT);
//        if (r.status() == RestStatus.OK) {
//            logger.info("count : {}", r.getCount());
//        }

        /**
         * 根据card_no 统计每个card_no 有多少数量
         */
        TermsAggregationBuilder field = AggregationBuilders.terms("gradeAgg").field("card_no");
        field.size(10);// 结果数据]
        /**
         * BucketOrder.count(boolean asc)
         按匹配文档格式升序/降序排序。
         BucketOrder.key(boolean asc)
         按key的升序或降序排序。
         BucketOrder.aggregation
         通过定义一个子聚合进行排序。
         BucketOrder.compound(List< BucketOrder> orders)
         创建一个桶排序策略，该策略根据多个条件对桶进行排序。
         */
        field.order(BucketOrder.count(false));

        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.size(0);// 每条明细数据不返回
        searchSourceBuilder.aggregation(field);

        SearchRequest sr = new SearchRequest("temp_asset_info");
        sr.source(searchSourceBuilder);

        SearchResponse response = client.search(sr, RequestOptions.DEFAULT);
        Terms aggregations = response.getAggregations().get("gradeAgg");
        for (Terms.Bucket bucket : aggregations.getBuckets()) {
            System.out.println(bucket.getKey() + ":" + bucket.getDocCount());
        }
    }


    @Test
    public void testGroupMaxMin() throws IOException {
        // 统计
        TermsAggregationBuilder tongji = AggregationBuilders.terms("gradeAgg").field("card_no");
        tongji.size(10);// 结果数据
        tongji.order(BucketOrder.count(false));
        // 统计后的值

        tongji.subAggregation(AggregationBuilders.max("maxId").field("id"));
        tongji.subAggregation(AggregationBuilders.min("minId").field("id"));

        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.size(0);// 每条明细数据不返回
        searchSourceBuilder.aggregation(tongji);

        SearchRequest sr = new SearchRequest("temp_asset_info");
        sr.source(searchSourceBuilder);

        SearchResponse response = client.search(sr, RequestOptions.DEFAULT);

        Terms aggregations = response.getAggregations().get("gradeAgg");
        for (Terms.Bucket bucket : aggregations.getBuckets()) {
            Max maxId = bucket.getAggregations().get("maxId");
            Min minId = bucket.getAggregations().get("minId");
            System.out.println(bucket.getKey() + ":" + bucket.getDocCount() + ":" + maxId.getValue() + ":"+ minId.getValue());
        }
    }
}
