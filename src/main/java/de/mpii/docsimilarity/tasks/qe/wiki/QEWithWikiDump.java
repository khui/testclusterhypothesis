package de.mpii.docsimilarity.tasks.qe.wiki;

import de.mpii.docsimilarity.mr.utils.StopWordsFilter;
import de.mpii.docsimilarity.mr.utils.io.ReadQuery;
import gnu.trove.map.TIntObjectMap;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Options;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.analysis.util.CharArraySet;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.similarities.LMDirichletSimilarity;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.canova.api.berkeley.StringUtils;

/**
 *
 * @author khui
 */
public class QEWithWikiDump {

    static Logger logger = Logger.getLogger(QEWithWikiDump.class.getName());

    private final Analyzer analyzer;

    private final String fieldname;

    private final DirectoryReader directoryReader;

    private float alpha = 1f, beta = 0.5f, decay = 0f;

    public QEWithWikiDump(String fieldname, String indexPath) throws ClassNotFoundException, InstantiationException, IllegalAccessException, IOException {
        analyzer = new StandardAnalyzer(new CharArraySet(Arrays.asList(StopWordsFilter.STOPWORDS), true));
        this.fieldname = fieldname;
        Directory dir = null;
                //FSDirectory.open(Paths.get(indexPath));
        directoryReader = DirectoryReader.open(dir);
    }

    public void setQEParameter(float alpha, float beta, float decay) {
        this.alpha = alpha;
        this.beta = beta;
        this.decay = decay;
    }

    public String queryExpansion(String querystr, int termNum, int docNum) throws IOException, ParseException {
        IndexSearcher searcher = new IndexSearcher(directoryReader);
        searcher.setSimilarity(new LMDirichletSimilarity());
        QueryParser qp = new QueryParser(fieldname, analyzer);
        Query query = qp.parse(querystr);
        ScoreDoc[] hits = searcher.search(query, docNum).scoreDocs;
        QueryExpansion qe = new QueryExpansion(analyzer, searcher, fieldname);
        qe.setParameters(termNum, docNum, alpha, beta, decay);
        String expandedquery = qe.expandQuery2str(querystr, hits);
        return expandedquery;
    }

    public static void main(String[] args) throws ClassNotFoundException, InstantiationException, IllegalAccessException, IOException, ParseException, org.apache.commons.cli.ParseException {
        Options options = new Options();
        options.addOption("o", "outfile", true, "output file");
        options.addOption("d", "dataORkeydirectory", true, "data/api key directory");
        options.addOption("i", "indexdirectory", true, "index directory");
        options.addOption("q", "queryfile", true, "query file");
        options.addOption("s", "meanstdscalefile", true, "scale parameters for feature normalization");
        options.addOption("l", "log4jxml", true, "log4j conf file");
        CommandLineParser parser = new BasicParser();
        CommandLine cmd = parser.parse(options, args);
        String outputf = null, indexdir = null, queryfile = null, log4jconf = null;
        if (cmd.hasOption("o")) {
            outputf = cmd.getOptionValue("o");
        }
        if (cmd.hasOption("l")) {
            log4jconf = cmd.getOptionValue("l");
        }
        if (cmd.hasOption("i")) {
            indexdir = cmd.getOptionValue("i");
        }
        if (cmd.hasOption("q")) {
            queryfile = cmd.getOptionValue("q");
        }

        org.apache.log4j.PropertyConfigurator.configure(log4jconf);
        LogManager.getRootLogger().setLevel(Level.INFO);
        QEWithWikiDump eqww = new QEWithWikiDump("title", indexdir);
        eqww.setQEParameter(1, 0.8f, 0f);

        TIntObjectMap<List<String>> qidQueryterms = ReadQuery.readQueries(queryfile);

        String expandedQ;
        StringBuilder sb;
        try (PrintStream ps = new PrintStream(outputf)) {
            for (int qid : qidQueryterms.keys()) {
                String query = StringUtils.join(qidQueryterms.get(qid), " ");
                expandedQ = eqww.queryExpansion(query, 100, 30);
                sb = new StringBuilder();
                sb.append(qid).append("\t");
                String[] termweights = expandedQ.split(" +");
                for (String termweight : termweights) {
                    String[] tw = termweight.split("\\^");
                    if (tw.length < 2) {
                        continue;
                    }
                    sb.append(tw[0]).append(":").append(tw[1]).append(" ");
                }

                ps.println(sb.toString());
            }
            ps.close();
        }
    }

}
