package sink;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch7.ElasticsearchSink;
import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;
import org.elasticsearch.client.RestHighLevelClient;
import utils.GlobalConfUtil;

import java.io.Serializable;
import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;


/*
* 一个将数据流写入到es的 自定义Sink
*
*
* */
public class ESSink<T> implements Serializable {

    private List<String> clusterName;
    private int port;
    private String scheme;
    private String index;

    public ESSink() {
        GlobalConfUtil conf = new GlobalConfUtil();
        this.clusterName = conf.getElasticsearch_cluster_hostname();
        this.port = conf.getElasticsearch_port();
        this.scheme = conf.getElasticsearch_scheme();
        this.index = conf.getElasticsearch_user_index();
    }

    public ElasticsearchSink<T> getSink() throws UnknownHostException {

        ArrayList<HttpHost> httpHosts = new ArrayList<>();
        for (String hostname : clusterName) {
            httpHosts.add(new HttpHost(hostname, port, scheme));
        }

        // 按照Flink对应的版本导入es依赖 使用过新的flink依赖可能出现依赖的包不存在错误
        ElasticsearchSink.Builder<T> esBuilder = new ElasticsearchSink.Builder<>(
                httpHosts,
                new MyElasticSearchSinkFunction()
        );
        // 参数配置 更多参数去官网了解
        esBuilder.setBulkFlushMaxActions(2);
        esBuilder.setRestClientFactory(
                restClientBuilder ->
                        restClientBuilder.setPathPrefix("es")
        );
        // 类需要实现 Java.io.Serializable 接口 该接口无需进行任何操作
        // java.lang.IllegalArgumentException: The implementation of the provided ElasticsearchSinkFunction is not serializable
        return esBuilder.build();
    }

    class MyElasticSearchSinkFunction implements ElasticsearchSinkFunction<T>, Serializable {

        @Override
        public void process(T t, RuntimeContext runtimeContext, RequestIndexer requestIndexer) {
            requestIndexer.add(createIndexRequest(t));
        }
    }


    public IndexRequest createIndexRequest(T elements) {
        Class<?> elementsClass = elements.getClass();
        //
        String id = "";
        HashMap<String, Object> map = new HashMap<>();
        // 获取所有属性 Declared:宣布
        // 通过解析JavaBean对象的注解 解析获取 id 和 属性名称 属性值
        Field[] fields = elementsClass.getDeclaredFields();
        for (Field field : fields) {
            field.setAccessible(true);
            String fieldName = field.getName();
            Object fieldValue = null;
            try {
                fieldValue = field.get(elements);
            } catch (IllegalAccessException e) {
                e.printStackTrace();
            }

            Annotation[] annotations = field.getAnnotations();
            for (Annotation annotation : annotations) {
                String annotationName = annotation.annotationType().getName();
                if (annotationName.equals("annotation.Id")) {
                    id = id.concat("-" + fieldValue);
                } else if (annotationName.equals("annotation.Column")) {
                    map.put(fieldName, fieldValue);
                }
            }
        }
        // id拼接多余的字符截取方式去除字符 如果你没有在JavaBean对象设置对应的注解 可能引发索引异常
        id = id.substring(1);
        return Requests.indexRequest()
                .index(index)
                .id(id)
                .opType("index")
                .source(map);
    }
}
