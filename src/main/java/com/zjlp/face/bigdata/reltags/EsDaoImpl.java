package com.zjlp.face.bigdata.reltags;

import com.zjlp.face.bigdata.utils.EsUtils;
import com.zjlp.face.bigdata.utils.Props;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import scala.Tuple2;
import scala.collection.Iterator;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

public class EsDaoImpl {
    private String esIndex = Props.get("es.index.reltags");
    private TransportClient client = EsUtils.getEsClient(Props.get("es.cluster.name"), Props.get("es.nodes"), Integer.valueOf(Props.get("es.client.port")));

    public Map<String, Integer> queryTags(String userId, List<String> otherUserIds) {
        SearchResponse response = client.prepareSearch(esIndex).setTypes(userId)
                .setQuery(QueryBuilders.idsQuery().ids(otherUserIds))
                .setExplain(false).execute().actionGet();
        SearchHit[] results = response.getHits().getHits();
        Map result = new HashMap();
        for (SearchHit sh : results) {
            result.put(sh.getId(), sh.getSource().get("tag"));
        }
        return result;
    }

    public void deleteESData(Iterator<Tuple2<String, String>> items) {
        if (items.isEmpty() || (!items.hasNext()) ) return;
        BulkRequestBuilder bulkRequest = client.prepareBulk();
        while (items.hasNext()) {
            Tuple2<String, String> item = items.next();
            bulkRequest.add(client.prepareDelete(esIndex, item._1(), item._2()));
        }
        bulkRequest.get();
    }

    public static void main(String[] args) {
        EsDaoImpl esDao = new EsDaoImpl();
        List<String> list = new ArrayList<String>();
        list.add("600");
        list.add("888888");
        list.add("4343");
        System.out.println(esDao.queryTags("602", list));
    }
}
