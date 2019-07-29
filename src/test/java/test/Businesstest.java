package test;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.BucketOrder;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.Cardinality;
import org.elasticsearch.search.aggregations.metrics.ValueCount;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.junit.Test;

import java.io.IOException;
import java.util.stream.Stream;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * 基于asset_info表做的测试
 *
 * @author: xupeng.guo
 * @date: 2019/6/6
 * @description
 */
public class Businesstest extends ElasticSearchTest {

    /**
     * 1. 根据资产ID获取资产信息
     * 2. 根据民族获取资产信息[汉字]
     * 3. 根据时间段查询资产信息
     * 4. 根据时间段统计该段时间内身份证号进件数量，对进件数量排序
     * 5. 根据姓名时间段及产品类型查询
     * 6. 根据渠道号分组计算个数 后排序
     * 7. select count(Distinct card_no) from temp_asset_info where create_time> xxx and create_time < yyyy and product_type_code=GXD
     * 8. select count(*) from temp_asset_info where create_time> xxx and create_time < yyyy and product_type_code=GXD
     * 9. select * from temp_asset_info where income_no in ('','');
     */


    private final String INDEX_NAME = "temp_asset_info";

    /**
     * 根据资产ID获取资产信息
     *
     * @throws IOException
     */
    @Test
    public void testGetAssetInfoById() throws IOException {
        // 1.先建立 Request
        SearchRequest searchRequest = new SearchRequest(INDEX_NAME);
        // 2. 建立SearchSourceBuilder
        SearchSourceBuilder sqb = new SearchSourceBuilder();
        sqb.query(QueryBuilders.termQuery("id", "90925"));
        // 3. request和SearchSourceBuilder关联
        searchRequest.source(sqb);
        // 4. 查询，可能抛出异常
        try {
            SearchResponse response = client.search(searchRequest, RequestOptions.DEFAULT);
            if (response.status() == RestStatus.OK) {
                SearchHits hits = response.getHits();
                assertNotNull(hits);
                SearchHit[] hits1 = hits.getHits();// 如果未找到,length=0
                Stream.of(hits1).forEach(x -> System.out.println("testGetAssetInfoById :" + x.getSourceAsString()));
            }
        } catch (ElasticsearchException e) {
            e.printStackTrace();
        }

    }


    /**
     * 根据民族获取资产信息[汉字]
     *
     * @throws IOException
     */
    public void testGetAssetInfosByNation() throws IOException {
        // 1.先建立 Request
        SearchRequest searchRequest = new SearchRequest(INDEX_NAME);
        // 2. 建立SearchSourceBuilder
        SearchSourceBuilder sqb = new SearchSourceBuilder();
        sqb.from(0);
        sqb.size(10);// 分页,每页10个
        sqb.query(QueryBuilders.matchQuery("nation", "汉"));
        // 3. request和SearchSourceBuilder关联
        searchRequest.source(sqb);
        // 4. 查询，可能抛出异常
        SearchResponse response = client.search(searchRequest, RequestOptions.DEFAULT);
        if (response.status() == RestStatus.OK) {
            SearchHits hits = response.getHits();
            assertNotNull(hits);
            SearchHit[] hitsEntities = hits.getHits();// 如果未找到,length=0
            logger.info("testGetAssetInfosByNation find data's size = {}.", hitsEntities.length);
            Stream.of(hitsEntities).forEach(x -> System.out.println("testGetAssetInfosByNation :" + x.getSourceAsString()));
        }
    }


    /**
     * 根据时间段查询资产信息
     *
     * @throws IOException
     */
    public void testGetAssetInfosByRangeDate() throws IOException {
        // 1.先建立 Request
        SearchRequest searchRequest = new SearchRequest(INDEX_NAME);
        // 2. 建立SearchSourceBuilder
        SearchSourceBuilder sqb = new SearchSourceBuilder();
        sqb.sort("create_time", SortOrder.DESC);//
        sqb.from(0);
        sqb.size(3);// 分页,每页3个
        RangeQueryBuilder timeRange = QueryBuilders.rangeQuery("create_time").from("2019-01-10 00:00:00").to("2019-01-11 00:00:00");
        timeRange.includeLower(false);
        timeRange.includeUpper(false);
        sqb.query(timeRange);
        // 3. request和SearchSourceBuilder关联
        searchRequest.source(sqb);
        // 4. 查询，可能抛出异常
        SearchResponse response = client.search(searchRequest, RequestOptions.DEFAULT);
        if (response.status() == RestStatus.OK) {
            SearchHits hits = response.getHits();
            assertNotNull(hits);
            SearchHit[] hitsEntities = hits.getHits();// 如果未找到,length=0
            logger.info("testGetAssetInfosByRangeDate find data's size = {}.", hitsEntities.length);
            Stream.of(hitsEntities).forEach(x -> System.out.println("testGetAssetInfosByRangeDate :" + x.getSourceAsString()));
        }
    }

    /**
     * 根据时间段统计该段时间内身份证号进件数量，对进件数量排序
     */
    public void testGetAssetInfoByMutiCondition1() throws IOException {
        SearchRequest searchRequest = new SearchRequest(INDEX_NAME);

        RangeQueryBuilder timeRange = QueryBuilders.rangeQuery("create_time").from("2019-01-10 00:00:00").to("2019-01-11 00:00:00");
        timeRange.includeLower(false);
        timeRange.includeUpper(false);

        TermsAggregationBuilder field = AggregationBuilders.terms("cardNumGroup").field("card_no");
        AggregationBuilders.dateRange("dateRange").addRange("2019-01-10 00:00:00", "2019-01-11 00:00:00");
        field.order(BucketOrder.count(false));
        field.size(1100);
        SearchSourceBuilder ssb = new SearchSourceBuilder();
        ssb.size(0);
        ssb.query(timeRange);
        ssb.aggregation(field);
        searchRequest.source(ssb);
        // 4. 查询，可能抛出异常
        SearchResponse response = client.search(searchRequest, RequestOptions.DEFAULT);
        if (response.status() == RestStatus.OK) {
            Terms terms = response.getAggregations().get("cardNumGroup");
            terms.getBuckets().forEach((bucket) -> {
                logger.info("testGetAssetInfoByMutiCondition results = {}.", bucket.getKey() + ":" + bucket.getDocCount());
            });
        }
    }

    /**
     * 根据姓名时间段及产品类型查询
     */
    public void testGetAssetInfoByMutiCondition2() throws IOException {
        SearchRequest searchRequest = new SearchRequest(INDEX_NAME);

        RangeQueryBuilder timeRange = QueryBuilders.rangeQuery("create_time").from("2019-01-10 00:00:00").to("2019-01-11 00:00:00");
        timeRange.includeLower(false);
        timeRange.includeUpper(false);


        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
        boolQueryBuilder.must(QueryBuilders.matchQuery("user_name", "5a1v9yNo78ACMNgxAexyUg=="));
        boolQueryBuilder.must(QueryBuilders.termQuery("channel_code", "BEIDOU"));
        boolQueryBuilder.must(timeRange);
        SearchSourceBuilder ssb = new SearchSourceBuilder();
        ssb.query(boolQueryBuilder);
        searchRequest.source(ssb);
        // 4. 查询，可能抛出异常
        SearchResponse response = client.search(searchRequest, RequestOptions.DEFAULT);
        if (response.status() == RestStatus.OK) {
            SearchHits hits = response.getHits();
            assertNotNull(hits);
            SearchHit[] hitsEntities = hits.getHits();// 如果未找到,length=0
            logger.info("testGetAssetInfoByMutiCondition2 find data's size = {}.", hitsEntities.length);
            Stream.of(hitsEntities).forEach(x -> System.out.println("testGetAssetInfoByMutiCondition2 :" + x.getSourceAsString()));
        }
    }

    /**
     * 根据渠道号和产品类型分组计算个数 后排序
     */
    public void testGetAssetInfoByChannelCodeProductTypeCode() throws IOException {
        SearchRequest searchRequest = new SearchRequest(INDEX_NAME);


        TermsAggregationBuilder field = AggregationBuilders.terms("channelCodeCount").field("channel_code");
        field.order(BucketOrder.count(false));
        field.size(1100);

        TermsAggregationBuilder field2 = AggregationBuilders.terms("productTypeCode").field("product_type_code");
        field.order(BucketOrder.count(false));
        field.size(1100);

        SearchSourceBuilder ssb = new SearchSourceBuilder();
        ssb.size(0);
        ssb.aggregation(field);
        searchRequest.source(ssb);
        // 4. 查询，可能抛出异常
        SearchResponse response = client.search(searchRequest, RequestOptions.DEFAULT);
        if (response.status() == RestStatus.OK) {
            Terms terms = response.getAggregations().get("channelCodeCount");
            terms.getBuckets().forEach((bucket) -> {
                logger.info("testGetAssetInfoByMutiCondition results = {}.", bucket.getKey() + ":" + bucket.getDocCount());
            });
        }
    }

    /**
     * 类似于select count(Distinct card_no) from temp_asset_info where create_time> xxx and create_time < yyyy and product_type_code=GXD
     */
    public void testGetAssetInfoCount1() throws IOException {
        SearchRequest searchRequest = new SearchRequest(INDEX_NAME);
        SearchSourceBuilder ssb = new SearchSourceBuilder();

        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
        boolQueryBuilder.must(QueryBuilders.rangeQuery("create_time").from("2019-01-10 00:00:00").to("2019-01-15 00:00:00"));
        boolQueryBuilder.filter(QueryBuilders.termQuery("product_type_code", "GXD"));
        // AggregationBuilders.cardinality 基于distinct的
        ssb.query(boolQueryBuilder).aggregation(AggregationBuilders.cardinality("realCount").field("card_no"));
        ssb.size(0);
        searchRequest.source(ssb);

        SearchResponse response = client.search(searchRequest, RequestOptions.DEFAULT);
        if (response.status() == RestStatus.OK) {
            Cardinality realCount = response.getAggregations().get("realCount");
            logger.info("testGetAssetInfoCount1,count = {}", realCount.getValue());
        }
    }

    /**
     * 类似于select count(*) from temp_asset_info where create_time> xxx and create_time < yyyy and product_type_code=GXD
     */
    public void testGetAssetInfoCount2() throws IOException {
        SearchRequest searchRequest = new SearchRequest(INDEX_NAME);
        SearchSourceBuilder ssb = new SearchSourceBuilder();

        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
        boolQueryBuilder.must(QueryBuilders.rangeQuery("create_time").from("2019-01-10 00:00:00").to("2019-01-15 00:00:00"));
        boolQueryBuilder.filter(QueryBuilders.termQuery("product_type_code", "GXD"));
        ssb.query(boolQueryBuilder).aggregation(AggregationBuilders.count("realCount").field("card_no"));
        ssb.size(0);
        searchRequest.source(ssb);

        SearchResponse response = client.search(searchRequest, RequestOptions.DEFAULT);
        if (response.status() == RestStatus.OK) {
            ValueCount realCount = response.getAggregations().get("realCount");
            logger.info("testGetAssetInfoByQueryConditionAndCountDistinct,count = {}", realCount.getValue());
        }
    }

    /**
     * select product_type_code,channel_code,count(1) from temp_asset_info group by product_type_code asc, channel_code desc
     */
    public void testGetAssetInfoGroup1() throws IOException {
        SearchRequest searchRequest = new SearchRequest(INDEX_NAME);

        TermsAggregationBuilder productTypeCode = AggregationBuilders.terms("productTypeCode").field("product_type_code");
        productTypeCode.order(BucketOrder.key(true));
        TermsAggregationBuilder channelCode = AggregationBuilders.terms("channelCode").field("channel_code");
        channelCode.order(BucketOrder.count(false));
        productTypeCode.subAggregation(channelCode);

        SearchSourceBuilder ssb = new SearchSourceBuilder();
        ssb.aggregation(productTypeCode);
        searchRequest.source(ssb);

        SearchResponse response = client.search(searchRequest, RequestOptions.DEFAULT);
        if (response.status() == RestStatus.OK) {
            Terms ptcTerms = response.getAggregations().get("productTypeCode");
            ptcTerms.getBuckets().forEach((ptcTerm) -> {
                Terms ccTerms = ptcTerm.getAggregations().get("channelCode");
                ccTerms.getBuckets().forEach((ccTerm) -> {
                    logger.info("productTypeCode = {}.channelCode={},count={}.", ptcTerm.getKey(), ccTerm.getKey(), ccTerm.getDocCount());
                });
            });
        }
    }

    /**
     * 9. select * from temp_asset_info where income_no in ('XHD_UC19010280000014553','XHD_UC19010480000015059');
     */
    public void testGetAssetInfoByInCondition() throws IOException {
        SearchRequest searchRequest = new SearchRequest(INDEX_NAME);
        SearchSourceBuilder ssb = new SearchSourceBuilder();
        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
        boolQueryBuilder.should(QueryBuilders.matchQuery("income_no", "XHD_UC19010280000014553"));
        boolQueryBuilder.should(QueryBuilders.matchQuery("income_no", "XHD_UC19010480000015059"));
        boolQueryBuilder.must(QueryBuilders.matchQuery("income_no", "XHD_UC19010480000015059"));
        ssb.query(boolQueryBuilder);
        searchRequest.source(ssb);
        SearchResponse response = client.search(searchRequest, RequestOptions.DEFAULT);
        if (response.status() == RestStatus.OK) {
            SearchHits hits = response.getHits();
            assertNotNull(hits);
            SearchHit[] hitsEntities = hits.getHits();// 如果未找到,length=0
            logger.info("testGetAssetInfoByInCondition find data's size = {}.", hitsEntities.length);
            Stream.of(hitsEntities).forEach(x -> System.out.println("testGetAssetInfoByInCondition :" + x.getSourceAsString()));
        }
    }

    /**
     * @throws IOException
     */
    @Test
    public void test() throws IOException {
        SearchRequest searchRequest = new SearchRequest(INDEX_NAME);
        SearchSourceBuilder ssb = new SearchSourceBuilder();
        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
        // 时间排序
        boolQueryBuilder.must(QueryBuilders.rangeQuery("create_time").from("2019-01-01 00:00:00").to("2019-07-15 00:00:00"));
        boolQueryBuilder.must(QueryBuilders.boolQuery().should(QueryBuilders.matchQuery("income_no", "XHD_UC19010480000015059"))
                .should(QueryBuilders.matchQuery("income_no", "XHD_UC19010280000014553")));

        ssb.query(boolQueryBuilder);
        searchRequest.source(ssb);
        SearchResponse response = client.search(searchRequest, RequestOptions.DEFAULT);
        if (response.status() == RestStatus.OK) {
            SearchHits hits = response.getHits();
            assertNotNull(hits);
            SearchHit[] hitsEntities = hits.getHits();// 如果未找到,length=0
            assertTrue(hitsEntities.length != 0);
            logger.info("testGetAssetInfoByInCondition find data's size = {}.", hitsEntities.length);
            Stream.of(hitsEntities).forEach(x -> System.out.println("testGetAssetInfoByInCondition :" + x.getSourceAsString()));
        }
    }
}
