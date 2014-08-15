/*
 * Geonames Solr Index - Harvester
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
package com.googlecode.solrgeonames.harvester;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.UnsupportedEncodingException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.lucene.search.BoostingQuery;
import org.apache.solr.client.solrj.embedded.EmbeddedSolrServer;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.CoreDescriptor;
import org.apache.solr.core.SolrConfig;
import org.apache.solr.core.SolrCore;
import org.apache.solr.core.SolrResourceLoader;
import org.apache.solr.schema.IndexSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A harvester to parse and index the geo data into an embedded Solr index.
 *
 * @author Greg Pendlebury
 */
public class Harvester {
    /** Logging */
    private static Logger log = LoggerFactory.getLogger(Harvester.class);

    /** Geonames uses tab-delimited files */
    private static String DELIMITER = "\t";

    /** Some constant counters */
    private static int BATCH_SIZE = 20000;

    /** Solr file names */
    private static String SOLR_CONFIG = "solrconfig.xml";
    private static String SOLR_SCHEMA = "schema.xml";

    /** If true, loading alternate names */
	private static boolean withAlternateNames;

	private static String geonamesFileName;

    /** Solr comma separated value of countries codes to boost. */
    private static List<String> countryIdsToBoost;
    
    /** Buffered Reader for line by line */
    private BufferedReader reader;

    /** Basic date formatter */
    private DateFormat dateFormat;

    /** Column mappings */
    private static final Map<String, Integer> columns;
    static {
        columns = new LinkedHashMap();
        columns.put("id",             0);
        columns.put("utf8_name",      1);
        columns.put("basic_name",     2);
        columns.put("alternate_names",3);
        columns.put("latitude",       4);
        columns.put("longitude",      5);
        columns.put("feature_class",  6);
        columns.put("feature_code",   7);
        columns.put("country_code",   8);
        // Skip other Country Codes : 9
        // Skip Admin Codes         : 10-13
        columns.put("population",     14);
        columns.put("elevation",      15);
        columns.put("gtopo30",        16);
        columns.put("timezone",       17);
        columns.put("date_modified",  18);
    }
    private List<String> columnsToExclude;

    /** Solr index */
    private SolrCore solrCore;
    private CoreContainer solrContainer;
    private EmbeddedSolrServer solrServer;

    /**
     * Basic constructor. Instantiate our reader and Solr.
     * @param sourceFile: The input file to read
     * @param countryIdsToBoost Country identifiers to boost. 
     * @param withAlternateNames Load alternate names field.
     *
     * @throws Exception if any errors occur
     */
    public Harvester(File sourceFile, List<String> countryIdsToBoost, List<String> columnsToExclude) throws Exception {
        this.countryIdsToBoost = countryIdsToBoost;
        this.columnsToExclude = columnsToExclude;
        
        // Variables
        InputStream inStream = null;
        Reader fileReader = null;
        
        // Open a stream to file
        try {
            inStream = new FileInputStream(sourceFile);
        } catch (IOException ex) {
            log.error("Error opening file stream!");
            throw new Exception(ex);
        }

        // Instantiate a UTF-8 reader from the stream
        try {
            fileReader = new InputStreamReader(inStream, "UTF-8");
        } catch (UnsupportedEncodingException ex) {
            try {
                inStream.close();
            } catch (IOException ioex) {
                log.error("Failed closing input stream");
            }
            log.error("Error starting file reader!");
            throw new Exception(ex);
        }

        reader = new BufferedReader(fileReader);

        // Time to bring Solr online
        // Find the Solr home
        String solrHome = System.getProperty("geonames.solr.home");
        if (solrHome == null) {
            throw new Exception("No 'geonames.solr.home' provided!");
        }
        solrServer = startSolr(solrHome);
    }

    /**
     * Start up an embedded Solr server.
     *
     * @param home: The path to the Solr home directory
     * @return EmbeddedSolrServer: The instantiated server
     * @throws Exception if any errors occur
     */
    private EmbeddedSolrServer startSolr(String home) throws Exception {
        try {
            SolrConfig solrConfig = new SolrConfig(home, SOLR_CONFIG, null);
            IndexSchema schema = new IndexSchema(solrConfig, SOLR_SCHEMA, null);

            solrContainer = new CoreContainer(new SolrResourceLoader(
                    SolrResourceLoader.locateSolrHome()));
            CoreDescriptor descriptor = new CoreDescriptor(solrContainer, "",
                    solrConfig.getResourceLoader().getInstanceDir());
            descriptor.setConfigName(solrConfig.getResourceName());
            descriptor.setSchemaName(schema.getResourceName());

            solrCore = new SolrCore(null, solrConfig.getDataDir(),
                    solrConfig, schema, descriptor);
            solrContainer.register("", solrCore, false);
            return new EmbeddedSolrServer(solrContainer, "");
        } catch(Exception ex) {
            log.error("\nFailed to start Solr server\n");
            throw ex;
        }
    }

    /**
     * Return the current date/time.
     *
     * @return Date: A Date object with the current date/time.
     */
    private Date now() {
        return new Date();
    }

    /**
     * Return a formatted time String of the current time.
     *
     * @return String: The current time String in the format HH:MM:SS
     */
    private String time() {
        return time(now());
    }

    /**
     * Return a formatted time String for the supplied Date.
     *
     * @param date: The Date object to format
     * @return String: The formatted time String in the format HH:MM:SS
     */
    private String time(Date date) {
        if (dateFormat == null) {
            dateFormat = new SimpleDateFormat("HH:mm:ss");
        }
        return dateFormat.format(date);
    }

    /**
     * Get the data indicated by the field name, after looking up the index
     * from the columns map.
     *
     * @param data: An array of strings containing column data
     * @param field: The field name
     * @return String: The data in that field, NULL if the field does not exist
     */
    private String get(String[] data, String field) {
        Integer index = columns.get(field);
        if (index == null) {
            log.error("Field does not exist: {}", field);
            return null;
        }
        return data[index];
    }

    /**
     * Force a commit against the underlying Solr database.
     *
     */
    private void commit() {
        try {
            solrServer.commit();
        } catch(Exception ex) {
            log.error("Failed to commit: ", ex);
        }
    }

    /**
     * Force an optimize call against the underlying Solr database.
     *
     */
    private void optimize() {
        try {
            solrServer.optimize();
        } catch(Exception ex) {
            log.error("Failed to commit: ", ex);
        }
    }

    /**
     * Main processing loop for the function
     *
     * @param counter: The number of rows to execute during this loop
     * @param print: Debugging flag to print all data processed
     * @return int: The number of rows read this pass
     * @throws Exception if any errors occur
     */
    public int loop(int counter, boolean print) throws Exception {
        String line = null;
        int i = 0;
        try {
            while (i < counter  && (line = reader.readLine()) != null) {
                String[] row = line.split(DELIMITER);

                i++;
                if (print) {
                    log.debug("====================");
                    log.debug("Line: {}", i);
                }
                process(row, print);
            }
        } catch (IOException ex) {
            throw new Exception(ex);
        }

        return i;
    }

    /**
     * Trivial test for empty Geonames data. Looks for null, empty strings,
     * or single space characters.
     *
     * @param input: The data to test
     * @return boolean: True if the data is consider 'empty', otherwise False
     */
    private boolean empty(String input) {
        if (input == null  || input.equals("") || input.equals(" ")) {
            return true;
        }
        return false;
    }

    /**
     * Process the row of data pulled from Geonames.
     *
     * @param row: A String array containing the columns of data
     * @param print: Debugging flag to print all data processed
     */
    private void process(String[] row, boolean print) {
        if (print) {
            for (String key : columns.keySet()) {
                System.out.format("%17s => %20s\n", key, get(row, key));
            }
        }
        try {
            solrServer.add(createSolrDoc(row));
        } catch(Exception ex) {
            log.error("Failed to add document:");
            for (String key : columns.keySet()) {
                System.out.format("%17s => %20s\n", key, get(row, key));
            }
            log.error("Stack trace: ", ex);
        }
    }

    /**
     * Create a Solr document from the provided Geonames column data.
     *
     * @param row: A String array containing the columns of data
     * @return SolrInputDocument: The prepared document
     */
    private SolrInputDocument createSolrDoc(String[] row) {
        float boost = 1.0f;

        SolrInputDocument doc = new SolrInputDocument();
        for (String key : columns.keySet()) {
        	if(columnsToExclude.contains(key)) {
        		continue;
        	}
        		
            String data = get(row, key);
            // Fix dates
            if (key.equals("date_modified")) {
                data += "T00:00:00Z";
            }
            // Sometimes the geonames 'asciiname' is empty
            if (key.equals("basic_name")) {
                if (empty(data)) {
                    data = get(row, "utf8_name");
                    //log.warn("{}: ASCII Name missing," +
                    //       " using UTF-8 version: '{}'", now(), data);
                }
                // We need a 'string' version, and a reversed thereof
                String string = data.toLowerCase();
                doc.addField("basic_name_str", string);
                String rev = new StringBuffer(string).reverse().toString();
                doc.addField("basic_name_rev", rev);
            }
            // Boost some countries
            if (countryIdsToBoost != null && key.equals("country_code")) {
                if (countryIdsToBoost.contains(data)) {
                    boost *= 5;
                }
            }
            // Boost populated locations
            if (key.equals("feature_code")) {
                if (data.startsWith("PPL")) {
                    boost *= 2;
                }
            }
            if (!empty(data)) {
                if (key.equals("alternate_names")) {
                    String[] alternate_names = data.split(",");
                    for (String alternate_name : alternate_names) {
                        doc.addField(key, alternate_name);
                    }
                }
                else {
                    doc.addField(key, data);
                }
            }
        }
        // We are placing the boost on a field that all records have the same
        //  value in. Then add 'AND boost:boost' to all queries.
        doc.addField("boost", "boost", boost);
        return doc;
    }

    /**
     * Shutdown function for cleaning up instantiated object.
     *
     */
    public void shutdown() {
        if (reader != null) {
            try {
                reader.close();
            } catch (IOException ex) {
                log.error("Error shutting down the Reader!", ex);
            }
        }
        if (solrContainer != null) {
            solrContainer.shutdown();
        }
    }

    /**
     * Command line entry point.
     *
     * @param args: Array of String parameters from the command line
     */
    public static void main(String[] args) {
    	if (args.length == 0) {
    		usage();
    	}
    	
    	// Eval input parameter
    	for (String arg : args) {
            evalParam(arg);
        }

        // Validate mandatory parameter
        File file = new File(geonamesFileName);
        if (file == null || !file.exists()) {
            log.error("ERROR: The input file does not exist!");
            usage();
            return;
        }
        
        // Exclude or not alternate names
        List<String> columnsToExclude = new ArrayList<String>();
        if (!withAlternateNames) {
        	columnsToExclude.add("alternate_names");
        }
                
        // Get ready to harvest
        Harvester harvester = null;
        try {
            harvester = new Harvester(file, countryIdsToBoost, columnsToExclude);

        } catch (Exception ex) {
            // A reason for death was logged in the constructor
            log.error("Stack trace: ", ex);
        }

        log.debug("\n\n===================\n\n");

        // Tracking variables
        Date start = harvester.now();
        int count = 0;

        // Run a single batch
        try {
            for (int i = 0; i < 500; i++) {
                int read = harvester.loop(BATCH_SIZE, false);
                count += read;
                log.info("{}: Rows read: {}", harvester.time(), count);

                // Commit after each batch
                try {
                    harvester.commit();
                } catch (Exception ex) {
                    log.info("Commit failed: {}", harvester.time());
                    log.error("Stack trace: ", ex);
                }

                // Did we finish?
                if (read != BATCH_SIZE) {
                    break;
                }
            }
        } catch (Exception ex) {
            log.error("ERROR: An error occurred in the processing loop: ", ex);
        }

        // Reporting
        Date finish = harvester.now();
        float duration = (float) (finish.getTime() - start.getTime()) / (float) 1000;
        log.info("\n\nTotal time for execution: {}", duration);
        log.info("Total records processed: {}", count);
        if (count == 0) {
            log.info("Average records per second: 0");
        } else {
            float speed = (float) count / (float) duration;
            log.info("Average records per second: {}", speed);
        }

        try {
            harvester.commit();
            log.info("\n{}: Index optimize...", harvester.time());
            harvester.optimize();
            log.info("{}: ... completed", harvester.time());
        } catch (Exception ex) {
            log.info("{}: ... failed", harvester.time());
            log.error("Stack trace: ", ex);
        }
        log.info("\n\n===================\n\n");

        harvester.shutdown();
    }

	private static void usage() {
		StringBuffer msg = new StringBuffer();
		msg.append("GeoNames solr harvester. Usage:\n");
		msg.append("  harvest.sh [--withAlternateNames] [--countryIdsToBoost=AU,FR] geonames-dump.txt\n");
		msg.append("\n");
		msg.append("    geonames-dump.txt: input file to ingest (mandatory). This input file is \n");
		msg.append("                       expected to be a tab delimited geonames data dump\n");
		msg.append("    --withAlternateNames: to load alternate names field\n");
		msg.append("    --countryIdsToBoost: a comma separated list of country identifiers to boost\n");
		log.info(msg.toString());		
	}

	private static void evalParam(String arg) {
		if (arg.startsWith("--withAlternateNames")) {
			withAlternateNames = true;
		} else if (arg.startsWith("--countryIdsToBoost")) {
			countryIdsToBoost = Arrays.asList(arg.split(","));
		} else {
			geonamesFileName = arg;
		}
		
	}
}
