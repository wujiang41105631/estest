package test;

import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * @author: xupeng.guo
 * @date: 2019/6/5
 * @description
 */
public class ElasticSearchTest {

    protected static Logger logger = LoggerFactory.getLogger(RequetTest.class);

    protected static RestHighLevelClient client = null;

    @BeforeClass
    public static void before() {
        logger.info("this is before");
        client = new RestHighLevelClient(
                RestClient.builder(
                        new HttpHost("10.255.72.213", 9200, "http"),
                        new HttpHost("10.255.72.214", 9200, "http"),
                        new HttpHost("10.255.72.215", 9200, "http")));
    }


    @AfterClass
    public static void after() {
        try {
            logger.info("this is end");
            client.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
