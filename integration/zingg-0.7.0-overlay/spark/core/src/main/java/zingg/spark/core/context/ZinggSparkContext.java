package zingg.spark.core.context;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataType;

import zingg.common.client.ZinggClientException;
//
import zingg.common.core.context.Context;
import zingg.spark.client.util.SparkDSUtil;
import zingg.spark.client.util.SparkModelHelper;
import zingg.spark.client.util.SparkPipeUtil;
import zingg.spark.core.util.SparkBlockingTreeUtil;
import zingg.spark.core.util.SparkGraphUtil;
import zingg.spark.core.util.SparkHashUtil;
import zingg.spark.core.util.SparkModelUtil;


public class ZinggSparkContext extends Context<SparkSession, Dataset<Row>, Row,Column,DataType>{

    
    private static final long serialVersionUID = 1L;
    public static final Log LOG = LogFactory.getLog(ZinggSparkContext.class);

	
    @Override
    public void init(SparkSession session)
        throws ZinggClientException {
			this.session = session;
        	setUtils();
		
    }

	@Override
	public void cleanup() {
        // Spark sessions/contexts are lifecycle-managed by Databricks. In particular
        // Serverless must not access or stop SparkContext from application code.
        session = null;
	}
    
    @Override
    public void setUtils() {
        LOG.debug("Session passed to utils is " + session);
        setPipeUtil(new SparkPipeUtil(session));
        setDSUtil(new SparkDSUtil(session));
        setHashUtil(new SparkHashUtil(session));
        setGraphUtil(new SparkGraphUtil());
        setModelUtil(new SparkModelUtil(session));
        setBlockingTreeUtil(new SparkBlockingTreeUtil(session, getPipeUtil()));
		setModelHelper(new SparkModelHelper());
    }

    
 }