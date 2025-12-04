package com.xiaoxj.sqlworkflow.remote.request;



import com.xiaoxj.sqlworkflow.remote.RequestHttpEntity;
import com.xiaoxj.sqlworkflow.remote.response.HttpClientResponse;

import java.io.Closeable;
import java.net.URI;

public interface HttpClientRequest extends Closeable {

  HttpClientResponse execute(URI uri, String httpMethod, RequestHttpEntity requestHttpEntity)
      throws Exception;
}
