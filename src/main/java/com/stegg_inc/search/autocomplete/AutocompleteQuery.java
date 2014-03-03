package com.stegg_inc.search.autocomplete;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.MalformedURLException;
import java.net.URLDecoder;
import java.util.List;

import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.request.FieldAnalysisRequest;
import org.apache.solr.client.solrj.response.AnalysisResponseBase.AnalysisPhase;
import org.apache.solr.client.solrj.response.AnalysisResponseBase.TokenInfo;
import org.apache.solr.client.solrj.response.FieldAnalysisResponse;
import org.apache.solr.client.solrj.response.FieldAnalysisResponse.Analysis;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.client.solrj.response.SpellCheckResponse;
import org.apache.solr.client.solrj.response.SpellCheckResponse.Collation;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.vertx.java.core.json.JsonObject;
import org.vertx.java.core.logging.Logger;
import org.vertx.java.core.logging.impl.LoggerFactory;

public class AutocompleteQuery {

	private Logger log;
	private String query;
	private SolrServer solr;

	private SpellCheckResponse spell;

	//private List<String> suggestions;
	private JsonObject suggestions;

	/**
	 * 
	 * @param query
	 * @param solr
	 * @param log
	 * @throws MalformedURLException
	 * @throws SolrServerException
	 */
	public AutocompleteQuery(String query, SolrServer solr)
			throws MalformedURLException, SolrServerException {

		log = LoggerFactory.getLogger(AutocompleteQuery.class);;
		this.query = query;
		this.solr = solr;
		suggestions = new JsonObject();

		ModifiableSolrParams params = new ModifiableSolrParams();

		params.set("q", cleanQuery());
		params.set("qt", "/suggest");
		params.set("fl", "spell");
		params.set("df", "spell");
		params.set("defType", "edismax");
		params.set("rows", "0");
		params.set("spellcheck.count", "5");
		params.set("spellcheck.collate", "true");
		params.set("spellcheck.maxCollations", "10");
		params.set("spellcheck.maxCollationTries", "1000");


		QueryResponse response = solr.query(params);

		spell = response.getSpellCheckResponse();

		if (spell != null) {
			processSuggestions();
		}
	}
	
	/**
	 * 
	 * @return
	 */
	private String cleanQuery() {
		if (query != null) {
			try {
				query = URLDecoder.decode(query, "UTF-8");
			} catch (UnsupportedEncodingException e) {
				log.error(e.getMessage());
			}
			return query;
		}
		return "";
	}

	/**
	 * 
	 */
	private void processSuggestions() {
		FieldAnalysisRequest far = new FieldAnalysisRequest();
		far.addFieldName("spell");
		far.setFieldValue(query);
		far.setQuery(query);
		try {
			FieldAnalysisResponse result = far.process(solr);
			Analysis analysis = result.getFieldNameAnalysis("spell");

			List<Collation> collations = spell.getCollatedResults();

			if(collations != null) {
				for(Collation collation : spell.getCollatedResults()) {
					String suggestion = collation.getCollationQueryString();
					String suggestion_highlighted = highlight(analysis, suggestion);
					
					suggestions.putString(suggestion, suggestion_highlighted);
				}
			}

		} catch (SolrServerException e) {
			log.error(e.getMessage());
		} catch (IOException e) {
			log.error(e.getMessage());
		}
	}

	/**
	 * 
	 * @param analysis
	 * @param suggestion
	 * @return
	 */
	private String highlight(Analysis analysis, String suggestion) {
		int qpc = analysis.getQueryPhasesCount();
		int i = 0;
		StringBuffer str = new StringBuffer(suggestion);

		for (AnalysisPhase ap : analysis.getQueryPhases()) {
			i++;
			// Only need to know the output of the last analyzer
			if (i < qpc) {
				continue;
			}
			List<TokenInfo> til = (List<TokenInfo>) ap.getTokens();
			
			TokenInfo ti = til.get(til.size()-1);
			
			// Finds index of part where to insert the <strong> tag
			int f = suggestion.indexOf(ti.getText()) + ti.getText().length();
			
			str.insert(f, "<strong>");
			str.append("</strong>");
		}
		return str.toString();
	}

	public JsonObject getSuggestions() {
		return suggestions;
	}

}