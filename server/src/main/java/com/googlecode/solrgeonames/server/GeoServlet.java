/*
 * Geonames Solr Index - Servlet
 * Copyright (C) 2011 University of Southern Queensland
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation; either version 2 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License along
 * with this program; if not, write to the Free Software Foundation, Inc.,
 * 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.
 */
package com.googlecode.solrgeonames.server;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.embedded.EmbeddedSolrServer;
import org.apache.solr.client.solrj.response.QueryResponse;

import org.apache.commons.lang.StringEscapeUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A basic servlet. The real work happens in the filter
 *
 * @author Greg Pendlebury
 */
public class GeoServlet extends HttpServlet {
    /** Logging */
    private static Logger log = LoggerFactory.getLogger(GeoServlet.class);

    public static String DEFAULT_START = "0";
    public static String DEFAULT_ROWS = "20";

    private EmbeddedSolrServer solrServer;

    /**
     * Initialise the Servlet, called at Server startup
     *
     * @throws ServletException If it found errors during startup
     */
    @Override
    public void init() throws ServletException {
        Object object = getServletContext().getAttribute("solr");
        if (object != null && object instanceof EmbeddedSolrServer) {
            solrServer = (EmbeddedSolrServer) object;
        } else {
            solrServer = null;
            log.error("Error accessing Solr from context");
        }
    }

    /**
     * Process an incoming GET request.
     *
     * @param request The incoming request
     * @param response The response object
     * @throws ServletException If errors found
     * @throws IOException If errors found
     */
    @Override
    protected void doGet(HttpServletRequest request,
            HttpServletResponse response) throws ServletException, IOException {
        String function = request.getParameter("func");
        if (function != null) {
            // A detail request for a specific entry
            if (function.equals("detail")) {
                detail(request, response);
                return;
            }
            // A search request
            if (function.equals("search") || function.equals("debug")) {
                // Suggest has the same data, but different output.
                search(request, response);
                return;
            }
        }

        // If things reached here no function was supplied
        OpenSearchResponse renderer = getRenderer(request);
        response.setStatus(400); // 400: Bad syntax
        response.setContentType(renderer.contentType());

        PrintWriter out = response.getWriter();
        out.println(renderer.renderError("No 'func' parameter was supplied"));
        out.close();
    }

    /**
     * Prepare a 'detail' query response
     *
     * @param request The incoming request
     * @param response The response object
     * @throws IOException If errors found
     */
    private void detail(HttpServletRequest req, HttpServletResponse resp)
            throws IOException {
        // Prepare a response
        PrintWriter out = resp.getWriter();
        OpenSearchResponse renderer = getRenderer(req);
        resp.setContentType(renderer.contentType());

        // Verify out data exists
        String id = req.getParameter("id");
        if (id == null || id.equals("")) {
            resp.setStatus(400); // 400: Bad syntax
            out.println(renderer.renderError(
                    "A detail query requires an 'id' parameter."));
            out.close();
            return;
        }

        // Run our query
        QueryResponse result = null;
        try {
            result = runQuery("id:"+id, 0, 1, "*,score", null, null, 0);

        } catch (Exception ex) {
            resp.setStatus(500); // 500: Server error
            StringWriter sw = new StringWriter();
            ex.printStackTrace(new PrintWriter(sw));
            out.println(renderer.renderError(
                    "An error occurred searching:\n"+sw.toString()));
            out.close();
            return;
        }

        // Render a response
        if (result.getResults().isEmpty()) {
            resp.setStatus(404); // 404: Not found
            out.println(renderer.renderEmptyResponse());
        } else {
            out.println(renderer.renderResponse(result));
        }
        out.close();
    }

    /**
     * Prepare a 'search' query response
     *
     * @param request The incoming request
     * @param response The response object
     * @throws IOException If errors found
     */
    private void search(HttpServletRequest req, HttpServletResponse resp)
            throws IOException  {
        // Prepare a response
        PrintWriter out = resp.getWriter();
        OpenSearchResponse renderer = getRenderer(req);
        resp.setContentType(renderer.contentType());

        // Did the submitter send a search team(s)
        String q = req.getParameter("q");
        String query = null;
        if (q == null || q.equals("")) {
            query = "boost:boost^10";
        }
        else {
            q = StringEscapeUtils.unescapeHtml(q);
        }

        // .. and a field to search in
        String field = req.getParameter("f");

        
        // Or build our query
        if (query == null) {
            query = buildWeightedQuery(q.toLowerCase(), field == null ? "basic_name" : field);
        }

        // Start index
        String start = req.getParameter("start");
        if (start == null || start.equals("")) {
            start = DEFAULT_START;
        }
        int iStart = Integer.valueOf(start);

        // Rows
        String rows = req.getParameter("rows");
        if (rows == null || rows.equals("")) {
            rows = DEFAULT_ROWS;
        }
        int iRows = Integer.valueOf(rows);

        // Run our query
        QueryResponse result = null;
        try {
            String func = req.getParameter("func");
            if (func.equals("debug")) {
                String[] facets = {"country_code", "feature_class", "feature_code"};
                String fq = req.getParameter("fq");
                result = runQuery(query, iStart, iRows,
                        "*,score", fq, facets, 100);
            } else {
                String fq = req.getParameter("fq");
                result = runQuery(query, iStart, iRows,
                        "*,score", fq, null, 0);
            }

        } catch (Exception ex) {
            resp.setStatus(500); // 500: Server error
            StringWriter sw = new StringWriter();
            ex.printStackTrace(new PrintWriter(sw));
            out.println(renderer.renderError(
                    "An error occurred searching:\n"+sw.toString()));
            out.close();
            return;
        }

        // Render a response
        if (result.getResults().isEmpty()) {
            out.println(renderer.renderEmptyResponse());
        } else {
            out.println(renderer.renderResponse(result));
        }
        out.close();
    }

    /**
     * Prepare a 'suggest' query response
     *
     * @param request The incoming request
     * @param response The response object
     * @throws IOException If errors found
     */
    private QueryResponse runQuery(String query, int start, int rows,
            String fields, String filter, String[] facets, int facetLimit)
            throws Exception {
        SolrQuery q = new SolrQuery();
        q.setQuery(query);
        q.setStart(start);
        q.setRows(rows);
        q.setFields(fields);
        if (filter != null) {
            q.setFilterQueries(filter);
        }
        if (facets != null) {
            q.setFacet(true);
            q.setFacetLimit(facetLimit);
            q.addFacetField(facets);
        } else {
            q.setFacet(false);
        }
        return solrServer.query(q);
    }

    /**
     * Construct a weighted query string for the provided search term
     *
     * @param q: The search term(s)
     * @return String: Constructed response String
     */
    private String buildWeightedQuery(String q, String field) {

        // Now some hardcoded boosting as we put it together
        String boost = "boost:boost^10";
        
    	if(field.equals("alternate_names")) {
    		String name = "(alternate_names:("+q+"*) OR alternate_names:("+q+"))";
    		return "("+name+")^0.2"+" AND "+boost;
    	} else {
	        String rev = new StringBuffer(q).reverse().toString();
	        // Perfect matches win
	        String both = "((basic_name_str:("+q+"*) AND basic_name_rev:("+rev+"*)) OR utf8_name:("+q+"))";
	        // Then left-anchored matches
	        String left = "(basic_name_str:("+q+"*) OR utf8_name:("+q+"*))";
	        // Then anything else
	        String name = "(basic_name:("+q+"*) OR basic_name:("+q+") OR alternate_names:("+q+"*) OR alternate_names:("+q+"))";
	        return "("+both+"^10 OR "+left+"^4 OR "+name+")^0.2"+" AND "+boost;
    	}
    }

    /**
     * Choose from among the six valid renderers based on used input and
     * defaults.
     *
     * @param request: The incoming HTTP request
     * @return OpenSearchResponse: An instantiated renderer
     */
    private OpenSearchResponse getRenderer(HttpServletRequest request) {
        String function = request.getParameter("func");
        String format = request.getParameter("format");

        if (format != null && format.equals("json")) {
            if (function != null && function.equals("detail")) {
                JsonDetailResponse renderer = new JsonDetailResponse();
                renderer.init(request);
                return renderer;
            }
            // Either search was requested, or something invalid
            JsonSearchResponse renderer = new JsonSearchResponse();
            renderer.init(request);
            return renderer;
        }
        // At this point the format is either invalid,
        // or HTML, we are using HTML either way
        if (function != null && function.equals("detail")) {
            HtmlDetailResponse renderer = new HtmlDetailResponse();
            renderer.init(request);
            return renderer;
        }
        if (function != null && function.equals("debug")) {
            HtmlDebugResponse renderer = new HtmlDebugResponse();
            renderer.init(request);
            return renderer;
        }
        // Either search was requested, or something invalid
        HtmlSearchResponse renderer = new HtmlSearchResponse();
        renderer.init(request);
        return renderer;
    }

    /**
     * Process an incoming POST request. In this case we simply redirect to GET
     *
     * @param request The incoming request
     * @param response The response object
     * @throws ServletException If errors found
     * @throws IOException If errors found
     */
    @Override
    protected void doPost(HttpServletRequest request,
            HttpServletResponse response) throws ServletException, IOException {
        doGet(request, response);
    }

    /**
     * Shuts down any objects requiring such.
     *
     */
    @Override
    public void destroy() {
        super.destroy();
    }
}
