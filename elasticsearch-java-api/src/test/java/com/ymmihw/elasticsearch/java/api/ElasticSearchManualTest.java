package com.ymmihw.elasticsearch.java.api;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.junit.Before;
import org.junit.Test;
import com.alibaba.fastjson.JSON;

public class ElasticSearchManualTest {
  private List<Person> listOfPersons = new ArrayList<>();
  private Client client = null;

  @Before
  public void setUp() throws UnknownHostException {
    Person person1 = new Person(10, "John Doe", new Date());
    Person person2 = new Person(25, "Janette Doe", new Date());
    listOfPersons.add(person1);
    listOfPersons.add(person2);
    Settings settings =
        Settings.builder().put("cluster.name", "elasticsearch").put("node.name", "fL6wjd2").build();
    client = new PreBuiltTransportClient(settings)
        .addTransportAddress(new TransportAddress(InetAddress.getByName("localhost"), 9300));
  }

  @Test
  public void givenJsonString_whenJavaObject_thenIndexDocument() {
    String jsonObject = "{\"age\":20,\"dateOfBirth\":1471466076564,\"fullName\":\"John Doe\"}";
    IndexResponse response =
        client.prepareIndex("people", "Doe").setSource(jsonObject, XContentType.JSON).get();
    String index = response.getIndex();
    String type = response.getType();
    assertTrue(response.status() == RestStatus.CREATED);
    assertEquals(index, "people");
    assertEquals(type, "Doe");
  }

  @Test
  public void givenDocumentId_whenJavaObject_thenDeleteDocument() {
    String jsonObject = "{\"age\":10,\"dateOfBirth\":1471455886564,\"fullName\":\"Johan Doe\"}";
    IndexResponse response =
        client.prepareIndex("people", "Doe").setSource(jsonObject, XContentType.JSON).get();
    String id = response.getId();
    DeleteResponse deleteResponse = client.prepareDelete("people", "Doe", id).get();
    assertTrue(deleteResponse.status() == RestStatus.OK);
  }

  @Test
  public void givenSearchRequest_whenMatchAll_thenReturnAllResults() {
    SearchResponse response = client.prepareSearch().execute().actionGet();
    SearchHit[] searchHits = response.getHits().getHits();
    List<Person> results = new ArrayList<>();
    for (SearchHit hit : searchHits) {
      String sourceAsString = hit.getSourceAsString();
      Person person = JSON.parseObject(sourceAsString, Person.class);
      results.add(person);
    }
  }

  @Test
  public void givenSearchParameters_thenReturnResults() {
    SearchResponse response =
        client.prepareSearch().setTypes().setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
            .setPostFilter(QueryBuilders.rangeQuery("age").from(5).to(15)).setFrom(0).setSize(60)
            .setExplain(true).execute().actionGet();

    SearchResponse response2 =
        client.prepareSearch().setTypes().setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
            .setPostFilter(QueryBuilders.simpleQueryStringQuery("+John -Doe OR Janette")).setFrom(0)
            .setSize(60).setExplain(true).execute().actionGet();

    SearchResponse response3 =
        client.prepareSearch().setTypes().setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
            .setPostFilter(QueryBuilders.matchQuery("John", "Name*")).setFrom(0).setSize(60)
            .setExplain(true).execute().actionGet();
    response2.getHits();
    response3.getHits();
    List<SearchHit> searchHits = Arrays.asList(response.getHits().getHits());
    final List<Person> results = new ArrayList<>();
    searchHits.forEach(hit -> results.add(JSON.parseObject(hit.getSourceAsString(), Person.class)));
  }

  @Test
  public void givenContentBuilder_whenHelpers_thanIndexJson() throws IOException {
    XContentBuilder builder = XContentFactory.jsonBuilder().startObject().field("fullName", "Test")
        .field("salary", "11500").field("age", "10").endObject();
    IndexResponse response = client.prepareIndex("people", "Doe").setSource(builder).get();
    assertTrue(response.status() == RestStatus.CREATED);
  }
}
