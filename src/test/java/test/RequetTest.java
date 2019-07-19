package test;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.get.MultiGetRequest;
import org.elasticsearch.action.get.MultiGetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.action.support.replication.ReplicationResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.index.reindex.UpdateByQueryRequest;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
import org.junit.Test;

import java.io.IOException;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

/**
 * @author: xupeng.guo
 * @date: 2019/5/17
 * @description
 */
public class RequetTest extends ElasticSearchTest {


    /**
     * CreateIndex
     */
//    @Test
    public void testIndexRequest() {
        // TODO: 2019/5/20  此处需要确定一个查询结果index.mapping下的dynamic字段为false时，
        // 预设的字段查出来的结果在es-head和 cerebro中不一致，userName是预设的，message不是预设的
        int id = 19;
        Map<String, Object> jsonMap = new HashMap<>();
        jsonMap.put("userName", "wang si");
        jsonMap.put("id", "" + id);
        jsonMap.put("postDate", new Date());
        jsonMap.put("message", "out");

        IndexRequest indexRequest = new IndexRequest("asset_info").id("" + id).source(jsonMap);
        indexRequest.timeout("1s");
        indexRequest.setRefreshPolicy(WriteRequest.RefreshPolicy.WAIT_UNTIL);
        indexRequest.opType(DocWriteRequest.OpType.CREATE);
        indexRequest.timeout(TimeValue.timeValueSeconds(1)); // 超时
        indexRequest.timeout("1s");

        try {
            IndexResponse indexResponse = client.index(indexRequest, RequestOptions.DEFAULT);
            logger.info("" + indexResponse.getResult());
            logger.info("" + indexResponse.getIndex());
            logger.info("" + indexResponse.getPrimaryTerm());
            logger.info("" + indexResponse.getSeqNo());
            logger.info("" + indexResponse.getVersion());

            ReplicationResponse.ShardInfo shardInfo = indexResponse.getShardInfo();
            if (shardInfo.getFailed() > 0) {
                for (ReplicationResponse.ShardInfo.Failure failure :
                        shardInfo.getFailures()) {
                    String reason = failure.reason();
                    logger.warn("create index error.reason = {}.", reason);
                }
            }
        } catch (ElasticsearchException e) {
            logger.error("", e);
            if (e.status() == RestStatus.CONFLICT) {

            }
        } catch (IOException e) {
            logger.error("IO Exception", e);
        }
    }

    /**
     * get API是实时的，并且不会受到索引刷新频率的影响。如果一个文档被更新了(update)，但是还没有刷新，那么get API将会发出一个刷新调用，以使文档可见。这也会使其他文档在上一次刷新可见后发生变化。如果不使用实时获取.
     * 详见 https://blog.csdn.net/prestigeding/article/details/83591529
     *
     * @throws IOException
     */
    @Test
    public void testGetRequest() {
        GetRequest getRequest = new GetRequest("asset_info", "1");
        getRequest.storedFields("userName"); //显示的指定需要返回的字段，默认会返回_source中所有字段。此属性需要将index mapping中对应的字段的store属性设置为true
//        FetchSourceContext fsc = new FetchSourceContext(true, new String[]{"userName1", "postDate1"}, null); //指定需要返回字段的上下文，是storedFields的补充与完善，支持通配符，下文会详细分析
//        getRequest.fetchSourceContext(fsc);
        getRequest.realtime(false);// get API是实时的，并且不会受到索引刷新频率的影响。如果一个文档被更新了(update)，但是还没有刷新，那么get API将会发出一个刷新调用，以使文档可见。这也会使其他文档在上一次刷新可见后发生变化。如果不使用实时获取，可以将realtime=false
        GetResponse documentFields = null;
        try {
            documentFields = client.get(getRequest, RequestOptions.DEFAULT);
            boolean exists = client.exists(getRequest, RequestOptions.DEFAULT);
            boolean exists2 = client.existsSource(getRequest, RequestOptions.DEFAULT);
            System.out.println(exists2);
            System.out.println(documentFields.getSeqNo());//  Elasticsearch中_seq_no的作用有两个，一是通过doc_id查询到该文档的seq_no，二是通过seq_no范围查找相关文档，所以也就需要存储为Index和DocValues（或者Store）。由于是在冲突检测时才需要读取文档的_seq_no，而且此时只需要读取_seq_no，不需要其他字段，这时候存储为列式存储的DocValues比Store在性能上更好一些。_seq_no是严格递增的，写入Lucene的顺序也是递增的，所以DocValues存储类型可以设置为Sorted。另外，_seq_no的索引应该仅需要支持存储DocId就可以了，不需要FREQS、POSITIONS和分词。如果多存储了这些，对功能也没影响，就是多占了一点资源而已。
            System.out.println(documentFields.getPrimaryTerm());//  _primary_term也和_seq_no一样是一个整数，每当Primary Shard发生重新分配时，比如重启，Primary选举等，_primary_term会递增1。_primary_term主要是用来恢复数据时处理当多个文档的_seq_no一样时的冲突，避免Primary Shard上的写入被覆盖。Elasticsearch中_primary_term只需要通过doc_id读取到即可，所以只需要保存为DocValues就可以了.
        } catch (ElasticsearchException e) {
            logger.error("", e);
            if (e.status() == RestStatus.CONFLICT) {

            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        // 结果处理
        String sourceAsString = documentFields.getSourceAsString();
        Map<String, Object> sourceAsMap = documentFields.getSourceAsMap();
        byte[] sourceAsBytes = documentFields.getSourceAsBytes();
        System.out.println(documentFields);
    }

    //    @Test
    public void testDeleteIndex() {
        try {
            System.out.println("---");
            DeleteResponse deleteResponse = client.delete(
                    new DeleteRequest("asset_info", "19"),
                    RequestOptions.DEFAULT);
            System.out.println("+++");
            System.out.println("status : " + deleteResponse.status());
        } catch (ElasticsearchException exception) {
            if (exception.status() == RestStatus.CONFLICT) {

            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testUpdateIndex() {
        Map<String, Object> jsonMap = new HashMap<>();
        jsonMap.put("postDate", new Date());
        jsonMap.put("message", "trying out Elasticsearch");
        UpdateRequest request = new UpdateRequest("asset_info1", "1")
                .doc(jsonMap);
        try {
            UpdateResponse update = client.update(request, RequestOptions.DEFAULT);
            if (update.status() == RestStatus.OK) {
                System.out.println("-----------------");
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    //    @Test
    public void testBulkRequest() throws InterruptedException, IOException {
        Map<String, Object> jsonMap = new HashMap<>();
        jsonMap.put("userName", "wang si");
        jsonMap.put("id", "30");
        jsonMap.put("postDate", new Date());
        jsonMap.put("message", "out");

        Map<String, Object> jsonMap2 = new HashMap<>();
        jsonMap2.putAll(jsonMap);
        jsonMap2.put("id", "1");

        Map<String, Object> jsonMap3 = new HashMap<>();
        jsonMap3.putAll(jsonMap);
        jsonMap3.put("id", "21");

        BulkRequest request = new BulkRequest();
        request.add(new IndexRequest("asset_info").id("30")
                .source(jsonMap).opType(DocWriteRequest.OpType.CREATE));
        request.add(new IndexRequest("asset_info").id("1")
                .source(jsonMap2).opType(DocWriteRequest.OpType.CREATE));
        request.add(new IndexRequest("asset_info").id("21")
                .source(jsonMap3).opType(DocWriteRequest.OpType.CREATE));

        BulkResponse bulk = client.bulk(request, RequestOptions.DEFAULT);
        if (bulk.hasFailures()) {
            BulkItemResponse[] items = bulk.getItems();
            for (BulkItemResponse item : items) {
                System.out.println(item.getFailureMessage());
            }
        }
//        BulkProcessor.Listener listener = new BulkProcessor.Listener() {
//            @Override
//            public void beforeBulk(long executionId, BulkRequest request) {
//
//            }
//
//            @Override
//            public void afterBulk(long executionId, BulkRequest request,
//                                  BulkResponse response) {
//
//            }
//
//            @Override
//            public void afterBulk(long executionId, BulkRequest request,
//                                  Throwable failure) {
//
//            }
//        };
//
//        BulkProcessor bulkProcessor = BulkProcessor.builder(
//                (r, bulkListener) ->
//                        client.bulkAsync(request, RequestOptions.DEFAULT, bulkListener),
//                listener).build();
//
//        BulkProcessor.Builder builder = BulkProcessor.builder(
//                (r, bulkListener) ->
//                        client.bulkAsync(request, RequestOptions.DEFAULT, bulkListener),
//                listener);
//        builder.setBulkActions(500);
//        builder.setBulkSize(new ByteSizeValue(1L, ByteSizeUnit.MB));
//        builder.setConcurrentRequests(0);
//        builder.setFlushInterval(TimeValue.timeValueSeconds(10L));
//        builder.setBackoffPolicy(BackoffPolicy
//                .constantBackoff(TimeValue.timeValueSeconds(1L), 3));
//
//        boolean terminated = bulkProcessor.awaitClose(30L, TimeUnit.SECONDS);
//        bulkProcessor.close();
    }

    /**
     * https://www.elastic.co/guide/en/elasticsearch/client/java-rest/7.0/java-rest-high-document-multi-get.html
     *
     * @throws IOException
     */
//    @Test
    public void testMutiGetRequest() throws IOException {
        MultiGetRequest request = new MultiGetRequest();
        request.add(new MultiGetRequest.Item(
                "asset_info",
                "1"));
        request.add(new MultiGetRequest.Item("asset_info", "2"));

        String[] includes = Strings.EMPTY_ARRAY;
        String[] excludes = new String[]{"foo", "*r"};
        FetchSourceContext fetchSourceContext =
                new FetchSourceContext(true, includes, excludes);
        request.add(new MultiGetRequest.Item("index", "example_id")
                .fetchSourceContext(fetchSourceContext));

        MultiGetResponse response = client.mget(request, RequestOptions.DEFAULT);

        System.out.println(response.isFragment());
        response.forEach((x) -> System.out.println(x.getResponse().getSourceAsString()));
    }

    /**
     * 有问题,set 字段哪搞
     * @throws IOException
     */
    @Test
    public void testUpdateByQueryRequest() throws IOException {
        UpdateByQueryRequest request =
                new UpdateByQueryRequest("asset_info");
        request.setConflicts("proceed");
        request.setQuery(new TermQueryBuilder("userName", "kimchy"));
        request.setRefresh(true);
        client.updateByQuery(request, RequestOptions.DEFAULT);
    }

}
