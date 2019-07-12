package com.ymmihw.elasticsearch;

import java.util.List;
import org.testcontainers.elasticsearch.ElasticsearchContainer;

public class MyElasticsearchContainer extends ElasticsearchContainer {
  private static final String IMAGE_VERSION = "elasticsearch:6.4.3";
  private static MyElasticsearchContainer container;

  private MyElasticsearchContainer() {
    super(IMAGE_VERSION);
  }

  public static MyElasticsearchContainer getInstance() {
    if (container == null) {
      container = new MyElasticsearchContainer();
    }
    return container;
  }

  @Override
  public void start() {
    List<String> env = getEnv();
    env.add("transport.host=0.0.0.0");
    env.add("cluster.name=elasticsearch");
    env.add("xpack.security.enabled=false");
    this.setEnv(env);
    super.start();
  }


  @Override
  public void stop() {
    // do nothing, JVM handles shut down
  }
}
