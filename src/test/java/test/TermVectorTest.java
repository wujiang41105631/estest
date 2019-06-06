package test;

import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.core.TermVectorsRequest;
import org.elasticsearch.client.core.TermVectorsResponse;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

/**
 * 关于TermVector 详见https://www.cnblogs.com/xing901022/p/5348737.html
 *                   https://blog.csdn.net/wangmaohong0717/article/details/80712978
 *
 * @author: xupeng.guo
 * @date: 2019/6/5
 * @description
 */

public class TermVectorTest extends ElasticSearchTest {
    /**
     * TermVectorsRequest  返回结果:词条的信息，比如position位置、start_offset开始的偏移值、end_offset结束的偏移值、词条的payLoads（这个主要用于自定义字段的权重）
     * 词条统计，doc_freq、ttf该词出现的次数、term_freq词的频率
     * 字段统计，包含sum_doc_freq该字段中词的数量(去掉重复的数目)、sum_ttf文档中词的数量（包含重复的数目）、doc_count涉及的文档数等等。
     */


    @Test
    public void termVectorTest() throws IOException {
//        TermVectorsRequest request = new TermVectorsRequest("authors", "1");
//        request.setFields("user");
        XContentBuilder docBuilder = XContentFactory.jsonBuilder();
        docBuilder.startObject().field("userName", "zhang").endObject();
        TermVectorsRequest request = new TermVectorsRequest("asset_info",
                docBuilder);
//        request.setFieldStatistics(false);
//        request.setTermStatistics(true);
//        request.setPositions(false);
//        request.setOffsets(false);
//        request.setPayloads(false);
//
//        Map<String, Integer> filterSettings = new HashMap<>();
//        filterSettings.put("max_num_terms", 3);
//        filterSettings.put("min_term_freq", 1);
//        filterSettings.put("max_term_freq", 10);
//        filterSettings.put("min_doc_freq", 1);
//        filterSettings.put("max_doc_freq", 100);
//        filterSettings.put("min_word_length", 1);
//        filterSettings.put("max_word_length", 10);
//
//        request.setFilterSettings(filterSettings);
//
//        Map<String, String> perFieldAnalyzer = new HashMap<>();
//        perFieldAnalyzer.put("user", "keyword");
//        request.setPerFieldAnalyzer(perFieldAnalyzer);
//
//        request.setRealtime(false);
//        request.setRouting("routing");

        TermVectorsResponse response =
                client.termvectors(request, RequestOptions.DEFAULT);
        String index = response.getIndex();
        String id = response.getId();
        boolean found = response.getFound();
        // Freq 表示频率
        for (TermVectorsResponse.TermVector tv : response.getTermVectorsList()) {
            String fieldname = tv.getFieldName();
            System.out.println(fieldname);
            int docCount = tv.getFieldStatistics().getDocCount();
            System.out.println(docCount);
            long sumTotalTermFreq = tv.getFieldStatistics().getSumTotalTermFreq();
            System.out.println(sumTotalTermFreq);
            long sumDocFreq = tv.getFieldStatistics().getSumDocFreq();
            System.out.println(sumDocFreq);
            if (tv.getTerms() != null) {
                List<TermVectorsResponse.TermVector.Term> terms =
                        tv.getTerms();
                for (TermVectorsResponse.TermVector.Term term : terms) {
//                    String termStr = term.getTerm();
//                    int termFreq = term.getTermFreq();
//                    int docFreq = term.getDocFreq();
//                    long totalTermFreq = term.getTotalTermFreq();
//                    float score = term.getScore();
                    if (term.getTokens() != null) {
                        List<TermVectorsResponse.TermVector.Token> tokens =
                                term.getTokens();
                        for (TermVectorsResponse.TermVector.Token token : tokens) {
                            int position = token.getPosition();
                            int startOffset = token.getStartOffset();
                            int endOffset = token.getEndOffset();
                            String payload = token.getPayload();
                        }
                    }
                }
            }
        }
    }
}
