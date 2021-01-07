package com.atguigu.es;

import com.atguigu.es.bean.MyRequestOptions;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.IndicesClient;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.*;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.TermsQueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightField;
import org.elasticsearch.search.sort.SortOrder;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.io.IOException;
import java.util.Map;

@SpringBootTest
class EsApplicationTests {

    @Autowired
    private RestHighLevelClient restHighLevelClient;


    /**
     * 高亮查询，对查询关键词进行高亮
     * 1.创建请求对象：高亮查询
     *    设置索引库name
     *    设置类型type
     * 2.创建查询请求体构建器
     *    设置请求体
     * 3.客户端发送请求，获取响应对象
     * 4.打印响应结果
     */
    @Test
    public void highLighterQuery() throws IOException {
        //1.创建请求对象
        SearchRequest request = new SearchRequest().types("_doc").indices("shopping01");
        //2.创建查询请求体构建器
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        //构建查询方式：高亮查询
        TermsQueryBuilder termsQueryBuilder = QueryBuilders.termsQuery("title","apple");
        //设置查询方式
        sourceBuilder.query(termsQueryBuilder);
        //构建高亮字段
        HighlightBuilder highlightBuilder = new HighlightBuilder();
        highlightBuilder.preTags("<font color='red'>");//设置标签前缀
        highlightBuilder.postTags("</font>");//设置标签后缀
        highlightBuilder.field("title");//设置高亮字段
        //设置高亮构建对象
        sourceBuilder.highlighter(highlightBuilder);
        //设置请求体
        request.source(sourceBuilder);
        //3.客户端发送请求，获取响应对象
        SearchResponse response = restHighLevelClient.search(request, RequestOptions.DEFAULT);
        //4.打印响应结果
        SearchHits hits = response.getHits();
        System.out.println("took::"+response.getTook());
        System.out.println("time_out::"+response.isTimedOut());
        System.out.println("total::"+hits.getTotalHits());
        System.out.println("max_score::"+hits.getMaxScore());
        System.out.println("hits::::>>");
        for (SearchHit hit : hits) {
            String sourceAsString = hit.getSourceAsString();
            System.out.println(sourceAsString);
            //打印高亮结果
            Map<String, HighlightField> highlightFields = hit.getHighlightFields();
            System.out.println(highlightFields);
        }
        System.out.println("<<::::");
    }

    /**
     * 高级查询
     */
    @Test
    public void boolAndRangeAndFuzzyQuery() throws IOException {
        //1.创建请求对象
        SearchRequest request = new SearchRequest().indices("shopping01").types("_doc");
        //构建查询的请求体
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        //高级查询的三种方式：
        //-----------------------------------------------------------------------------
//        //bool查询：查询title中必须包含小米，一定不含有电视，应该含有手机的所有商品
//        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
//        //must
//        boolQueryBuilder.must(QueryBuilders.matchQuery("title", "小米"));
//        //must not
//        boolQueryBuilder.mustNot(QueryBuilders.matchQuery("title", "电视"));
//        //should
//        boolQueryBuilder.should(QueryBuilders.matchQuery("title", "手机"));
//        sourceBuilder.query(boolQueryBuilder);
        //-----------------------------------------------------------------------------
        //范围查询：查询价格大于3千，小于5千的所有商品
//        RangeQueryBuilder rangeQuery = QueryBuilders.rangeQuery("price");
//        //#### gt 大于(greater than)
//        rangeQuery.gt("3000");
//        //#### lt 小于(less than)
//        rangeQuery.lt("5000");
//        //#### gtelte 大于等于(greater than equals)
//        //####  小于等于(less than equals)
//        sourceBuilder.query(rangeQuery);
        //-----------------------------------------------------------------------------
        //模糊查询：查询包含apple关键词的所有商品，完成模糊查询cpple
       sourceBuilder.query(QueryBuilders.fuzzyQuery("title","ccple").fuzziness(Fuzziness.TWO));
        //-----------------------------------------------------------------------------
        request.source(sourceBuilder);
        //2.客户端发送请求，获取响应对象
        SearchResponse response = restHighLevelClient.search(request, RequestOptions.DEFAULT);
        //3.打印结果信息
        printResult(response);
    }

    /**
     * 目标：查询的字段过滤，分页，排序
     *
     * @throws IOException
     */
    @Test
    public void fetchSourceAndSortAndByPage() throws IOException {
        //1.创建请求对象
        SearchRequest request = new SearchRequest().indices("shopping01").types("_doc");
        //构建查询的请求体
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        //查询所有
        sourceBuilder.query(QueryBuilders.matchAllQuery());
        //分页信息
        //当前页其实索引(第一条数据的顺序号)，from
        sourceBuilder.from(2);
        //每页显示多少条size
        sourceBuilder.size(2);
        //排序信息,参数一：排序的字段，参数二：顺序ASC升序，降序DESC
        sourceBuilder.sort("price", SortOrder.ASC);
        //查询字段过滤
        String[] excludes = {};
        String[] includes = {"title", "subtitle", "price"};
        sourceBuilder.fetchSource(includes, excludes);
        request.source(sourceBuilder);
        //2.客户端发送请求，获取响应对象
        SearchResponse response = restHighLevelClient.search(request, RequestOptions.DEFAULT);
        //3.打印结果信息
        printResult(response);
    }

    @Test
    public void basicQuery() throws IOException {
        SearchRequest searchRequest = new SearchRequest("shopping01");
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        //searchSourceBuilder.query(QueryBuilders.matchAllQuery());
        //match查询，带分词器的查询
       // searchSourceBuilder.query(QueryBuilders.matchQuery("title","小米手机"));
        searchSourceBuilder.query(QueryBuilders.termQuery("price", "1999"));
        searchRequest.source(searchSourceBuilder);
        SearchResponse response = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
        //3.打印结果信息
        printResult(response);
    }
    private void printResult(SearchResponse response) {
        SearchHits hits = response.getHits();
        System.out.println("took:" + response.getTook());
        System.out.println("timeout:" + response.isTimedOut());
        System.out.println("total:" + hits.getTotalHits());
        System.out.println("MaxScore:" + hits.getMaxScore());
        System.out.println("hits========>>");
        for (SearchHit hit : hits) {
            //输出每条查询的结果信息
            System.out.println(hit.getSourceAsString());
        }
        System.out.println("<<========");
    }

    @Test
    public void initData() throws IOException {
        //批量新增操作
        BulkRequest request = new BulkRequest();
        request.add(new IndexRequest().type("_doc").index("shopping01").source(XContentType.JSON, "title", "小米手机", "images", "http://www.gulixueyuan.com/xm.jpg", "price", 1999.0));
        request.add(new IndexRequest().type("_doc").index("shopping01").source(XContentType.JSON, "title", "小米电视", "images", "http://www.gulixueyuan.com/xmds.jpg", "price", 2999.0));
        request.add(new IndexRequest().type("_doc").index("shopping01").source(XContentType.JSON, "title", "华为手机", "images", "http://www.gulixueyuan.com/hw.jpg", "price", 4999.0, "subtitle", "小米"));
        request.add(new IndexRequest().type("_doc").index("shopping01").source(XContentType.JSON, "title", "apple手机", "images", "http://www.gulixueyuan.com/appletl.jpg", "price", 5999.00));
        request.add(new IndexRequest().type("_doc").index("shopping01").source(XContentType.JSON, "title", "apple", "images", "http://www.gulixueyuan.com/apple.jpg", "price", 3999.00));
        BulkResponse response = restHighLevelClient.bulk(request, RequestOptions.DEFAULT);
        System.out.println("took::" + response.getTook());
        System.out.println("Items::" + response.getItems());
    }

    //批量删除
    @Test
    public void testBatchDeleteDocument() throws IOException {
        BulkRequest bulkRequest = new BulkRequest();
        bulkRequest.add(new DeleteRequest("abc","_doc","1"));
        bulkRequest.add(new DeleteRequest("abc","_doc","2"));
        bulkRequest.add(new IndexRequest("abc").type("_doc").id("4")
                .source(XContentType.JSON,"field", "baz"));

        restHighLevelClient.bulk(bulkRequest,RequestOptions.DEFAULT);
        restHighLevelClient.close();
    }
    //文档的批量添加与批量删除操作
    @Test
    public void testBatchDocument() throws IOException {
        BulkRequest bulkRequest = new BulkRequest();
        bulkRequest.add(new IndexRequest("abc","_doc","1").source(XContentType.JSON,"id",1,"title","a"));
        bulkRequest.add(new IndexRequest("abc","_doc","2").source(XContentType.JSON,"id",2,"title","b"));
        bulkRequest.add(new IndexRequest("abc","_doc","3").source(XContentType.JSON,"id",3,"title","c"));

        restHighLevelClient.bulk(bulkRequest,RequestOptions.DEFAULT);
        restHighLevelClient.close();
    }

    //文档的修改
    @Test
    public void testDocumentUpdate() throws IOException {
       // UpdateRequest updateRequest = new UpdateRequest("abc","_doc","2");
        //GetRequest getRequest=new GetRequest("abc","_doc","2");
        DeleteRequest deleteRequest = new DeleteRequest("abc","_doc","2");
      //  Product product = new Product(3L,"或者",33.0,"aa.jpg");
      // String result = JSON.toJSONString(product);
//        indexRequest.source(result,XContentType.JSON);

        //updateRequest.upsert(result, XContentType.JSON);
        //updateRequest.doc(XContentType.JSON, "id", "2", "title", "小米手机", "price", "2999");
        //发送请求,得到响应结果
        //IndexResponse response = restHighLevelClient.index(indexRequest, RequestOptions.DEFAULT);
      //  UpdateResponse update = restHighLevelClient.update(updateRequest, RequestOptions.DEFAULT);
       // GetResponse documentFields = restHighLevelClient.get(getRequest, RequestOptions.DEFAULT);
        DeleteResponse delete = restHighLevelClient.delete(deleteRequest, RequestOptions.DEFAULT);

        ////3.打印结果信息
        System.out.println("_index:" + delete.getIndex());
        System.out.println("_type:" + delete.getType());
        System.out.println("_id:" + delete.getId());
        System.out.println("_result:" + delete.getResult());
    }

    //文档的添加
    @Test
    public void testDocumentAdd() throws IOException {
        IndexRequest indexRequest=new IndexRequest("abc","_doc","2");
//        Product product = new Product(2L,"或者",33.0,"aa.jpg");
//        String result = JSON.toJSONString(product);
//        indexRequest.source(result,XContentType.JSON);
        indexRequest.source(XContentType.JSON,"id","3","title","您好");
        //发送请求,得到响应结果
        IndexResponse response = restHighLevelClient.index(indexRequest, RequestOptions.DEFAULT);

        ////3.打印结果信息
        System.out.println("_index:" + response.getIndex());
        System.out.println("_type:" + response.getType());
        System.out.println("_id:" + response.getId());
        System.out.println("_result:" + response.getResult());
    }

    //映射获取
    @Test
    public void testGettingMapping() throws IOException {
        IndicesClient indices = restHighLevelClient.indices();
        GetMappingsRequest request=new GetMappingsRequest();
        request.indices("abc");

        GetMappingsResponse response = indices.getMapping(request, RequestOptions.DEFAULT);
        Map<String, MappingMetaData> mappings = response.mappings();
        MappingMetaData abc = mappings.get("abc");
        Map<String, Object> map = abc.sourceAsMap();
        Map<String, Object> properties = ( Map<String, Object>)map.get("properties");
        Map<String, Object> map3 = ( Map<String, Object>)properties.get("images");

        System.out.println("map3:"+map3);
    }

    //映射的设置与获取
    //映射的设置
    @Test
    public void testPuttingMapping() throws IOException {

        PutMappingRequest putMappingRequest = new PutMappingRequest("abc");

        putMappingRequest.source("{\n" +
                "  \"properties\": {\n" +
                "    \"title\":{\n" +
                "      \"type\": \"text\",\n" +
                "      \"analyzer\": \"ik_max_word\"\n" +
                "    },\n" +
                "    \"subtitle\":{\n" +
                "      \"type\": \"text\",\n" +
                "      \"analyzer\": \"ik_max_word\"\n" +
                "    },\n" +
                "    \"images\":{\n" +
                "      \"type\": \"keyword\",\n" +
                "      \"index\": false\n" +
                "    },\n" +
                "    \"price\":{\n" +
                "      \"type\": \"float\",\n" +
                "      \"index\": true\n" +
                "    }\n" +
                "  }\n" +
                "}",XContentType.JSON);


        IndicesClient indices = restHighLevelClient.indices();
        AcknowledgedResponse acknowledgedResponse = indices.putMapping(putMappingRequest, RequestOptions.DEFAULT);

        System.out.println("acknowledgedResponse"+acknowledgedResponse.isAcknowledged());

    }



    //查看索引信息
    @Test
    public void testIndexInfo() throws IOException {
        IndicesClient indices = restHighLevelClient.indices();
        GetIndexRequest request = new GetIndexRequest("ddd");
        GetIndexResponse getIndexResponse = indices.get(request, RequestOptions.DEFAULT);
        System.out.println(getIndexResponse.getAliases());
        System.out.println(getIndexResponse.getMappings());
        System.out.println(getIndexResponse.getSettings());
        System.out.println("分片数:"+getIndexResponse.getSetting("ddd", "index.number_of_shards"));
    }

    //查看索引是否存在
    @Test
    public void testIndexExists() throws IOException {
        IndicesClient indices = restHighLevelClient.indices();
        GetIndexRequest getIndexRequest = new GetIndexRequest("ddd");
        boolean exists = indices.exists(getIndexRequest, MyRequestOptions.COMMON_OPTIONS);
        System.out.println(exists);
    }
    //删除索引
    @Test
    public void testDeleteIndex() throws IOException {
        IndicesClient indices = restHighLevelClient.indices();
        DeleteIndexRequest deleteIndexRequest= new DeleteIndexRequest("abc");
        AcknowledgedResponse delete = indices.delete(deleteIndexRequest, RequestOptions.DEFAULT);

        System.out.println(delete.isAcknowledged());
    }

    //创建索引
    @Test
    void contextLoads() throws IOException {
        IndicesClient indices = restHighLevelClient.indices();
        CreateIndexRequest request = new CreateIndexRequest("shopping01");
        request.settings(Settings.builder()
                .put("index.number_of_shards", 3)
                .put("index.number_of_replicas", 2)
        );
        //调用indices.create
        CreateIndexResponse createIndexResponse = indices.create(request, RequestOptions.DEFAULT);
        System.out.println(createIndexResponse.index());
        System.out.println(createIndexResponse.isAcknowledged());
        System.out.println(createIndexResponse.isShardsAcknowledged());

        //关闭高级客户端
        restHighLevelClient.close();
    }

}
