/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package solr;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.MalformedURLException;
import java.util.ArrayList;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.MultiThreadedHttpConnectionManager;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CommonsHttpSolrServer;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;


class SolrReqThread extends Thread {
    static int notindexed;
    static int ops_sec;
    static int secs, fieldlength;
    static int id;

    
    static int nwords;
    static int results;
    private int [] docs;
    private int [] time;

    static int total, mode;
    static String [] palavras;
    static CommonsHttpSolrServer server;


    public SolrReqThread(int i, int ops) {
        id = i;
        ops_sec = ops;
        docs = new int[1000];
        time = new int[1000];
    }

    public void run() {
        SolrQuery query = new SolrQuery();
        int beg = 1;//(int)(Math.random() * 10);
        query.setStart(beg);
        query.setRows(results);

        for (int k = 0;k < secs; k++) {
            long tempo = 0;
            for (int l = 0;l < ops_sec; l++) {
                String s = tune_string_by_size();
                query.setQuery(s);
                try {
                    tempo += getResponse(server, query, k);
                } catch (SolrServerException ex) {
                    Logger.getLogger(SolrReqThread.class.getName()).log(Level.SEVERE, null, ex);
                }
            }
            if (tempo > 1050) {
                //System.out.println("CUT: " + (k+1) + " (Thread " + id + ": " + tempo + " > 1s)");
                time[k] = -1;
            }
            while (tempo < 950) {
                long start = System.currentTimeMillis();
                try {
                    sleep( (int)((1000 - tempo) * 0.8) );
                } catch (InterruptedException ex) {
                    Logger.getLogger(SolrReqThread.class.getName()).log(Level.SEVERE, null, ex);
                }
                long end = System.currentTimeMillis();
                tempo += end - start;
            }
        }
    }

    private synchronized long getResponse(CommonsHttpSolrServer server, SolrQuery query,int s) throws SolrServerException {
        QueryResponse rsp;
        long start = System.currentTimeMillis();
        rsp = server.query(query);
        long end   = System.currentTimeMillis();
        
        time[s] += end - start;
        docs[s] += rsp.getResults().size();
        return end - start;
    }
    
    private static String tune_string_by_size() {
        int rand = (int) ( Math.random() * total );
        int len = palavras[rand].length();
        String word =  palavras[rand];
        while (len < fieldlength) {
            word += "-" + palavras[rand];
            len  += 1   + palavras[rand].length();
        }
        return word.substring(0, fieldlength);
    }

    public int getTime(int t) {
        return time[t];
    }
    public int getDocs(int t) {
        return docs[t];
    }
}


public class SolrSearch {

    int total = 0;
    float tam_medio = 0;
    String [] palavras = new String[500000];

    String dictionary_path, index_path;
    static int ops_per_sec, num_threads, nwords, variable, secs;
    static int fieldlength, notindexed;
    static int results;
    boolean order;

    public void lerPalavras() {
        File file = new File(dictionary_path);
        FileInputStream fis = null;
        BufferedInputStream bis = null;
        DataInputStream dis = null;

        try {
            fis = new FileInputStream(file);

            // Here BufferedInputStream is added for fast reading.
            bis = new BufferedInputStream(fis);
            dis = new DataInputStream(bis);

            while (dis.available() != 0) {
                palavras[total++] = dis.readLine();
                tam_medio += palavras[total-1].length();
            }
            tam_medio /= total;

            // dispose all the resources after using them.
            fis.close();
            bis.close();
            dis.close();
        } catch (FileNotFoundException e) {
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public int searchdata(CommonsHttpSolrServer server) throws SolrServerException, IOException, Exception {
        SolrReqThread.secs        = secs;
        SolrReqThread.nwords      = nwords;
        SolrReqThread.palavras    = palavras;
        SolrReqThread.total       = total;
        SolrReqThread.fieldlength = fieldlength;
        SolrReqThread.notindexed  = notindexed;
        SolrReqThread.server      = server;
        SolrReqThread.results     = results;

        SolrReqThread.palavras  = palavras;
        SolrReqThread.total     = total;


        int operations = ops_per_sec;
        ArrayList<SolrReqThread> threads = new ArrayList<SolrReqThread>();
        for (int k = 0;k < num_threads; k++) {
            int ops_thread = operations / (num_threads - k);

            threads.add(new SolrReqThread(k, ops_thread));
            operations -= ops_thread;
        }
        for (int k = 0;k < num_threads; k++)
            threads.get(k).start();
        for (int k = 0;k < num_threads; k++)
            threads.get(k).join();

        float [] docs = new float[10000];
        float [] time = new float[10000];

        float d_mean = (float) 0.0, t_mean = (float) 0.0;
        boolean error = false;
        for (int k = 1;k < secs; k++) {
            docs[k] = 0;
            time[k] = 0;
            for (int l = 0;l < num_threads; l++) {
                docs[k] += threads.get(l).getDocs(k);
                if (threads.get(l).getTime(k) < 0) {
                    time[k] = -1.0f;
                    error = true;
                    break;
                }
                else
                    time[k] += threads.get(l).getTime(k);
            }
            t_mean += time[k];
            d_mean += docs[k];
            time[k] /= ops_per_sec;
            docs[k] /= ops_per_sec;
            //System.out.println("Second " + k + ": " + docs[k] + " e " + time[k]);
        }

        if (variable == 0) {
            float m = (float) 0.0;
            int s = 0;
            for (int k = 0;k < secs; k++)
                if (time[k] > 0) {
                    m += time[k];
                    s++;
                }
            m /= s;
            System.out.println(s + ";" + m);
        }
        if (variable == 1) {
            if (time[1] < 0)
                return 0;
            t_mean = time[1];
            d_mean = docs[1];
            t_mean /= (secs * ops_per_sec);
            d_mean /= (secs * ops_per_sec);
            System.out.println(num_threads + ";" + t_mean + ";" + d_mean);
        }
        if (variable == 2) {
            if (time[1] < 0) {
                System.out.println("Problem: " + ops_per_sec);
                return 0;
            }
            t_mean = time[1];
            d_mean = docs[1];
            time[1] /= ((secs - 1) * ops_per_sec);
            docs[1] /= ((secs - 1) * ops_per_sec);
            System.out.println(ops_per_sec + ";" + t_mean + ";" + d_mean);
        }
        return 1;
    }
    
    
    // ============================== START FUNCTIONS ==================================
    public void search_seconds(int e) throws SolrServerException, IOException, Exception {
        CommonsHttpSolrServer server = new CommonsHttpSolrServer("http://localhost:8080/solr/");
        variable    = 0;
        secs        = e + 1;
        ops_per_sec = 50;
        num_threads = 5 + 1;
        
        searchdata(server);
    }

    public void search_threads(int t) throws SolrServerException, Exception {
        CommonsHttpSolrServer server = new CommonsHttpSolrServer("http://localhost:8080/solr/");
        variable        = 1;
        secs            = 1 + 1;
        ops_per_sec     = 1500;

        for (int k=10;k<=t;k++) {
            num_threads = k;
            if (searchdata(server) == 0)
                k--;
            Thread.sleep(1000);
        }
    }

    public void search_opspersec(int ops) throws MalformedURLException, SolrServerException, Exception {
        CommonsHttpSolrServer server = new CommonsHttpSolrServer("http://localhost:8080/solr/");
        variable        = 2;
        num_threads     = 20;
        secs            = 1 + 1;
        ops_per_sec     = ops;

        for (int k=100;k <= ops; k += 100) {
            ops_per_sec = k;
            if (searchdata(server) == 0)
                k -= 100;
            Thread.sleep(1000);
        }
    }

    public void search_results(int res) throws MalformedURLException, SolrServerException, InterruptedException, IOException, Exception {
        CommonsHttpSolrServer server = new CommonsHttpSolrServer("http://localhost:8080/solr/");
        variable        = 3;
        num_threads     = 10;
        secs            = 10 + 1;
        ops_per_sec     = 100;

        for (int k=1;k <= res; k ++) {
            results = k;
            if (searchdata(server) == 0)
                k--;
            Thread.sleep(1000);
        }
    }

    public SolrSearch(String[] args) throws SolrServerException, IOException, Exception {
        // Parametros
        ops_per_sec   = Integer.parseInt(args[0]);
        num_threads   = Integer.parseInt(args[1]);
        nwords        = Integer.parseInt(args[2]);
        secs          = Integer.parseInt(args[3]);
        fieldlength   = Integer.parseInt(args[4]);
        notindexed    = Integer.parseInt(args[6]);
        results       = Integer.parseInt(args[7]);

        index_path    = "/opt/ptin/ach/apache-tomcat-6.0.32/bin/solr/data/index";

        // Dicionario
        dictionary_path = "dics/dic_" + args[5] + ".txt";
        lerPalavras();
    }
}
