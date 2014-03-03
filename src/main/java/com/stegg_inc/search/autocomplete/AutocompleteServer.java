package com.stegg_inc.search.autocomplete;

import java.io.IOException;
import java.net.MalformedURLException;

import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudSolrServer;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.vertx.java.core.Handler;
import org.vertx.java.core.http.HttpServer;
import org.vertx.java.core.http.HttpServerRequest;
import org.vertx.java.core.json.JsonObject;
import org.vertx.java.platform.Verticle;
import org.vertx.java.core.logging.Logger;
import org.vertx.java.core.logging.impl.LoggerFactory;

public class AutocompleteServer extends Verticle {

	HttpServer server;
	Logger log;

	HttpSolrServer solrHttp;
	CloudSolrServer solrCloud;
	SolrServer solr;
	
	// Config params
	String conf_zkhosts;
	String conf_collection;
	String conf_solrserver;
	int conf_contimeout;
	int conf_sotimeout;

	String conf_listenHost = "127.0.0.1";
	int conf_listenPort = 8081;

	JsonObject appConfig;

	private void init() {

		log = LoggerFactory.getLogger(AutocompleteServer.class);
		

		try {
			appConfig = container.config();
			
			conf_zkhosts = (String)appConfig.getValue("zkHosts");
			conf_collection = (String)appConfig.getValue("defaultCollection");
			conf_solrserver = (String)appConfig.getValue("solrServer");
			conf_contimeout = (int)appConfig.getValue("connectionTimeout");
			conf_sotimeout = (int)appConfig.getValue("socketTimeout");
			
			
			if (!((String)appConfig.getValue("listenHost")).isEmpty())
			{
				conf_listenHost = (String)appConfig.getValue("listenHost");
			}
			if((Integer)appConfig.getValue("listenPort") != null)
			{
				conf_listenPort = (int)appConfig.getValue("listenPort");
			}
				
			if (conf_zkhosts != null) {
				solrCloud = new CloudSolrServer(conf_zkhosts);
				solrCloud.setDefaultCollection(conf_collection);
				solrCloud.getLbServer().setConnectionTimeout(conf_contimeout);
				solrCloud.getLbServer().setSoTimeout(conf_sotimeout);
			} else if (conf_solrserver != null) {
				solrHttp = new HttpSolrServer(conf_solrserver);
				solrHttp.setBaseURL(conf_solrserver + "/" + conf_collection);
				solrHttp.setConnectionTimeout(conf_contimeout);
				solrHttp.setSoTimeout(conf_sotimeout);
			} else {
				throw new Exception(
						"Parameters not set correctly in config.json");
			}
			
		} catch (IOException ex) {
			log.error(ex.getMessage());
		} catch (Exception ex) {
			log.error(ex.getMessage());
		}
	}
	
	@Override
	public void start() {
		// Initialize properties
		init();

		server = vertx.createHttpServer();
		
		log.info("Starting autocomplete server on " + conf_listenHost + ":" + conf_listenPort);
		
		server.requestHandler(new Handler<HttpServerRequest>() {
			public void handle(final HttpServerRequest request) {
				request.exceptionHandler(new Handler<Throwable>() {
		            public void handle(Throwable t) {
		            	request.response().end("");
		            	log.error("Request failed");
		            	log.error(request.toString());
		            }
		        });
				request.response().putHeader("content-type", "text/plain");

				try {
					String query = request.params().get("query");
					String collection = request.params().get("collection");
					if(collection == null) {
						collection = conf_collection;
						if(collection == null) {
							//throw MalformedURLException
						}		
					}

					if(solrHttp != null)
					{
						solrHttp.setBaseURL(conf_solrserver + "/" + collection);
						solr = solrHttp;
					}
					else if (solrCloud != null)
					{
						solrCloud.setDefaultCollection(collection);
						log.info("Default collection: " + solrCloud.getDefaultCollection());
						solr = solrCloud;
					}
					
					AutocompleteQuery aq = new AutocompleteQuery(query, solr);

					String suggestions = aq.getSuggestions().toString();

					request.response().putHeader("Content-Length", Integer.toString(suggestions.length()));
					request.response().putHeader("Content-Type", "text/plain; charset=utf-8");
					request.response().write(suggestions);

				} catch (MalformedURLException e) {
					log.error(e.getMessage());
				} catch (SolrServerException e) {

					log.error(e.getMessage());
				}

				request.response().end();
				request.response().close();
			}
		}).listen(conf_listenPort, conf_listenHost);
	}

	/**
	 * 
	 */
	@Override
	public void stop() {
		server.close();
	}
}