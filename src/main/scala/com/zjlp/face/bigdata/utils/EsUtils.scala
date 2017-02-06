package com.zjlp.face.bigdata.utils

import java.net.InetAddress

import org.elasticsearch.client.transport.TransportClient
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.common.transport.InetSocketTransportAddress

object EsUtils {
  def getEsClient(clusterName: String, hosts: String, port: Int = 9300): TransportClient = {
    val settings: Settings = Settings.settingsBuilder.put("cluster.name", clusterName).put("client.transport.sniff", true).build
    val client: TransportClient = TransportClient.builder.settings(settings).build
    hosts.split(",").foreach {
      host =>
        client.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(host), port))

    }
    return client
  }
}
