package test;

import org.elasticsearch.action.fieldcaps.FieldCapabilities;
import org.elasticsearch.action.fieldcaps.FieldCapabilitiesRequest;
import org.elasticsearch.action.fieldcaps.FieldCapabilitiesResponse;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.RequestOptions;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;

/**
 * @author: xupeng.guo
 * @date: 2019/6/6
 * @description
 */
public class FieldCapabilitiesRequestTest extends ElasticSearchTest{

    @Test
    public void testCapabilitiesRequest() throws IOException {
        FieldCapabilitiesRequest request = new FieldCapabilitiesRequest()
                .fields("userName")
                .indices("asset_info", "asset_info1");
        request.indicesOptions(IndicesOptions.lenientExpandOpen());

        FieldCapabilitiesResponse fieldCapabilitiesResponse = client.fieldCaps(request, RequestOptions.DEFAULT);
        Map<String, FieldCapabilities> userResponse = fieldCapabilitiesResponse.getField("userName");
        FieldCapabilities textCapabilities = userResponse.get("text");

        boolean isSearchable = textCapabilities.isSearchable();
        System.out.println(isSearchable);
        boolean isAggregatable = textCapabilities.isAggregatable();
        System.out.println(isAggregatable);
        String[] indices = textCapabilities.indices();
        System.out.println(Arrays.toString(indices));
        String[] nonSearchableIndices = textCapabilities.nonSearchableIndices();
        System.out.println(Arrays.toString(nonSearchableIndices));
        String[] nonAggregatableIndices = textCapabilities.nonAggregatableIndices();
        System.out.println(Arrays.toString(nonAggregatableIndices));

    }
}
