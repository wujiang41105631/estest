package test;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.junit.Test;

import java.io.IOException;
import java.util.stream.Stream;

import static org.junit.Assert.assertNotNull;

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
    @Test
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
    @Test
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
}
