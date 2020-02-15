package org.bonitasoft.grumman.message;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Logger;

import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import javax.sql.DataSource;

import org.bonitasoft.engine.api.ProcessAPI;
import org.bonitasoft.engine.bpm.flownode.ActivityDefinition;
import org.bonitasoft.engine.bpm.flownode.BoundaryEventDefinition;
import org.bonitasoft.engine.bpm.flownode.CatchEventDefinition;
import org.bonitasoft.engine.bpm.flownode.CatchMessageEventTriggerDefinition;
import org.bonitasoft.engine.bpm.flownode.FlowElementContainerDefinition;
import org.bonitasoft.engine.bpm.flownode.IntermediateCatchEventDefinition;
import org.bonitasoft.engine.bpm.process.DesignProcessDefinition;
import org.bonitasoft.engine.bpm.process.ProcessDefinitionNotFoundException;
import org.bonitasoft.engine.expression.Expression;
import org.bonitasoft.engine.operation.Operation;
import org.bonitasoft.log.event.BEvent;
import org.bonitasoft.log.event.BEvent.Level;
import org.bonitasoft.log.event.BEventFactory;

public class MessagesFactory {

    final static Logger logger = Logger.getLogger(MessagesFactory.class.getName());

    public static String loggerLabel = "MessagesFactory ##";

    private final static BEvent eventSqlQuery = new BEvent(MessagesFactory.class.getName(), 1, Level.ERROR, "SQL Query error", "Error during a SQL Query", "No value available", "check exception");

    /* -------------------------------------------------------------------- */
    /*                                                                      */
    /* getSynthesis */
    /*                                                                      */
    /* -------------------------------------------------------------------- */
    private final String sqlQueryCountWaitingEvent = "select count(*) as nbevents from waiting_event";
    private final String sqlQueryCountMessageInstance = "select count(*) as nbinstances from message_instance";
    private final String sqlQueryCountReconciliation = "select distinct count(*) " +
            "from " +
            "waiting_event f " +
            "message_instance m " +
            "where " +
            "AND m.locked = 0 " +
            "AND f.locked = 0 " +
            "AND m.handled = 0 " +
            "AND f.active = 1 " +
            "AND f.progress = 0 " +
            "and f.processname = m.targetprocess " +
            "and f.flownodename = m.targetflownode " +
            "and f.CORRELATION1 = m.CORRELATION1 " +
            "and f.CORRELATION2 = m.CORRELATION2 " +
            "and f.CORRELATION3 = m.CORRELATION3 " +
            "and f.CORRELATION4 = m.CORRELATION4 " +
            "and f.CORRELATION5 = m.CORRELATION5 ";

    private final static String sqlQueryReconciliation = "select distinct w.eventtype as W_eventtype, " +
            " m.id as \"M_id\", w.id as \"W_id\"," +
            " fi.lastupdatedate as W_lastupdatedate, w.ROOTPROCESSINSTANCEID as W_rootprocessintance," +
            " w.processname as W_processname,w.locked as W_locked, w.active as W_active, w.progress as W_progress,  pd.version as W_version,  " +
            " pd.processid as W_processdefinitionid, w.flownodename as W_flownodename, w.CORRELATION1 as W_correlation1, w.CORRELATION2 as W_correlation2, " +
            " w.CORRELATION3 as W_correlation3, w.CORRELATION4  as W_correlation4, w.CORRELATION5  as W_correlation5,  " +
            " m.locked as M_locked, m.handled as M_handled " +
            " from waiting_event w " +
            " left join flownode_instance fi on (w.flownodeinstanceid = fi.id) " +
            " left join process_instance pi on (pi.id = w.ROOTPROCESSINSTANCEID) " +
            " left join process_definition pd on (pi.processdefinitionid = pd.processid), " +
            " message_instance m " +
            " where m.locked = 0 AND w.locked = 0 and w.active = 1 AND w.progress = 0 " +
            " and w.processname = m.targetprocess " +
            " and w.flownodename = m.targetflownode" +
            " and w.CORRELATION1 = m.CORRELATION1 " +
            " and w.CORRELATION2 = m.CORRELATION2 " +
            " and w.CORRELATION3 = m.CORRELATION3 " +
            " and w.CORRELATION4 = m.CORRELATION4 " +
            " and w.CORRELATION5 = m.CORRELATION5 " +
            " order by W_lastupdatedate";

    private final String sqlQueryLoadMessageContent = "select * from data_instance where containertype='MESSAGE_INSTANCE' and containerid=?";

    public class SynthesisOnMessage {

        long nbWaitingEvent;
        long nbMessageEvent;
        long nbReconciliation;
        List<BEvent> listEvents = new ArrayList<>();

        public Map<String, Object> getMap() {
            Map<String, Object> result = new HashMap<String, Object>();
            result.put("nbwaitingevent", nbWaitingEvent);
            result.put("nbmessageevent", nbMessageEvent);
            result.put("nbreconciliation", nbReconciliation);
            result.put("listevents", BEventFactory.getSyntheticHtml(listEvents));
            return result;
        }

    }

    public SynthesisOnMessage getSynthesis() {
        SynthesisOnMessage synthesisOnMessage = new SynthesisOnMessage();

        try (Connection con = getConnection();) {
            synthesisOnMessage.nbWaitingEvent = getLong(executeOneResultQuery(sqlQueryCountWaitingEvent, null, 0L, con));
            synthesisOnMessage.nbMessageEvent = getLong(executeOneResultQuery(sqlQueryCountMessageInstance, null, 0L, con));
            synthesisOnMessage.nbReconciliation = getLong(executeOneResultQuery(sqlQueryCountReconciliation, null, 0L, con));

        } catch (Exception e) {
            synthesisOnMessage.listEvents.add(new BEvent(eventSqlQuery, e, ""));
        }
        return synthesisOnMessage;
    }

    /* -------------------------------------------------------------------- */
    /*                                                                      */
    /* Message */
    /*                                                                      */
    /* -------------------------------------------------------------------- */

    public class Message {

        public Map<String, Object> waitingEvent = new HashMap<String, Object>();
        public Long waitingId;
        public Map<String, Object> messageInstance = new HashMap<String, Object>();
        public Long messageId;

        public Map<String, Object> messageContentVariables = new HashMap<String, Object>();
        /**
         * when the message is incomplete, this is the complete one
         */
        public Map<String, Object> completeMessage;
        public boolean isComplete = true;

        public StringBuilder incompleteDetail= new StringBuilder();
        public int nbWaitingEvent;
        public int nbMessageContent;
        public String processName;
        public String processVersion;
        public Long processDefinitionId;
        public String flowNodeName;
        public Long dateWaitingEvent;

        public int nbExecutionsInProgress = 0;
        public int nbExecutionsWithSuccess = 0;

        public String toString() {
            return processName + "(" + processVersion + ")-" + flowNodeName + ", date:" + dateWaitingEvent + ", nbWaitingEvent:" + nbWaitingEvent + " nbMessageContent:" + nbMessageContent;
        }
    }

    /* -------------------------------------------------------------------- */
    /*                                                                      */
    /* operations on message */
    /*                                                                      */
    /* -------------------------------------------------------------------- */

    public List<Message> getListReconcilationMessages(int numberOfMessages) {
        List<Message> listMessages = new ArrayList<>();
        try (Connection con = getConnection();) {
            List<Map<String, Object>> listRecords = executeListResultQuery(sqlQueryReconciliation, null, numberOfMessages, con);
            for (Map<String, Object> record : listRecords) {
                Message message = new Message();
                for (String key : record.keySet()) {
                    if (key.startsWith("w_"))
                        message.waitingEvent.put(key.substring(2), record.get(key));
                    if (key.startsWith("m_"))
                        message.messageInstance.put(key.substring(2), record.get(key));
                }
                message.processName = (String) record.get( "w_processname");
                message.processVersion = (String) record.get( "w_version");
                message.flowNodeName = (String) record.get( "w_flownodename");
                message.dateWaitingEvent = (Long) record.get( "w_lastupdatedate");
                message.processDefinitionId = (Long) record.get( "w_processdefinitionid");
                message.messageId = (Long) record.get( "m_id");
                message.waitingId = (Long) record.get( "w_id" );
                listMessages.add(message);
            }

        } catch (Exception e) {
            logger.severe(loggerLabel + " Exception " + e.toString());
        }

        return listMessages;
    }

    






    
    
    // we don't manage CLOB and BLOB
    private static String[] listColumDatainstance = { "intvalue", "longvalue", "shorttextvalue", "booleanvalue", "doublevalue", "floatvalue" };

    public Map<String, Object> loadMessageContentData(Message message, boolean loadValues) {

        try (Connection con = getConnection();) {
            List<Map<String, Object>> listRecords = executeListResultQuery(sqlQueryLoadMessageContent, Arrays.asList(message.messageId), 100, con);
            for (Map<String, Object> record : listRecords) {
                Object value = null;
                if (loadValues) {
                    for (String columName : listColumDatainstance) {
                        if (record.get(columName) != null)
                            value = record.get(columName);
                    }

                }
                message.messageContentVariables.put((String) record.get("name"), value);
            }

        } catch (Exception e) {
            logger.severe(loggerLabel + " Exception " + e.toString());
        }

        return message.messageContentVariables;
    }

    private Map<String,Set<String>> cacheDesign = new HashMap<>();
    /**
     * @param processDefinitionId
     * @param taskName
     * @return
     */
    public Set<String> loadDesignContentData(long processDefinitionId, String taskName, ProcessAPI processAPI) {
        
        try {
            String keyCache =  processDefinitionId+"#"+taskName;
            Set<String> design = cacheDesign.get(keyCache);
            if (design!=null)
                return design;
            
            DesignProcessDefinition designProcessAPI = processAPI.getDesignProcessDefinition(processDefinitionId);
            FlowElementContainerDefinition flowElementContainer = designProcessAPI.getFlowElementContainer();
            // maybe an activity or a intermediate catch event, Boundary event, Activity
            ActivityDefinition activityMessage = null;
            BoundaryEventDefinition boundaryMessage = null;
            IntermediateCatchEventDefinition eventMessage = null;

            
            List<ActivityDefinition> listActivities=  flowElementContainer.getActivities();
            for (ActivityDefinition activity : listActivities)
            {
                if (activity.getName().equals(taskName))
                    activityMessage = activity;
            
            List<BoundaryEventDefinition> listBoundaries= activity.getBoundaryEventDefinitions();
            for (BoundaryEventDefinition boundaryEvent : listBoundaries)
            {
                if (boundaryEvent.getName().equals(taskName))
                    boundaryMessage= boundaryEvent;
            }
            }
             
            List<IntermediateCatchEventDefinition> listCatchEvent = flowElementContainer.getIntermediateCatchEvents();
            for (IntermediateCatchEventDefinition catchEvent : listCatchEvent) 
            {
                if (catchEvent.getName().equals(taskName)) {
                    eventMessage = catchEvent;
                }
            }
                
            if (activityMessage == null && boundaryMessage==null && eventMessage ==null) {
                cacheDesign.put(keyCache, null);
                return null;
            }
            design=new HashSet<>();
            // message can be generated from a ThrowEvent or from the API. 
            // when it's generate from the API, no way to access definition. So, look on operation, and collect the data here
            List<Operation> listOperations = new ArrayList<>();
            CatchEventDefinition catchEventDefinition = null;
            if (eventMessage!=null)
                catchEventDefinition = eventMessage;
            if (boundaryMessage !=null)
                catchEventDefinition = boundaryMessage;
            
            
            if (catchEventDefinition!=null) {
                List<CatchMessageEventTriggerDefinition> listEventsTrigger = catchEventDefinition.getMessageEventTriggerDefinitions();
                for(CatchMessageEventTriggerDefinition triggerDefinition : listEventsTrigger)
                {
                    listOperations.addAll( triggerDefinition.getOperations() );
                }
            }
            if (activityMessage !=null)
                listOperations.addAll( activityMessage.getOperations());
            
            // now look operation
            for (Operation operation : listOperations)
            {
                Expression rightOperand= operation.getRightOperand();
                if (rightOperand.getExpressionType().equals("TYPE_VARIABLE"))
                    design.add(rightOperand.getContent());
            }
            cacheDesign.put(keyCache, design);
            return design;
        
        } catch (ProcessDefinitionNotFoundException e) {
            logger.severe(loggerLabel + " Exception " + e.toString());
        }
        
        return null;

    }

    public void loadNumberOfRecords(Message message) {
        // search, for the correlation, 
        // how many waiting_event are present
        // how many message_instance are present

    }

    public List<BEvent> executeMessage(Message message, Map<String, Object> contentMessage) {
        return new ArrayList<>();
    }

    public List<BEvent> purgeMessageInstance(Message message, int nbRecordToPurge) {
        return new ArrayList<>();
    }

    /* -------------------------------------------------------------------- */
    /*                                                                      */
    /* getConnection */
    /*                                                                      */
    /* -------------------------------------------------------------------- */
    private static List<String> listDataSources = Arrays.asList("java:/comp/env/bonitaSequenceManagerDS",
            "java:jboss/datasources/bonitaSequenceManagerDS");

    /**
     * @param query
     * @param parameters
     * @param defaultValue
     * @param con
     * @return
     */
    public Object executeOneResultQuery(String query, List<Object> parameters, Object defaultValue, Connection con) {
        ResultSet rs = null;
        try (PreparedStatement pstmt = con.prepareStatement(query);) {
            if (parameters != null) {
                for (int i = 0; i < parameters.size(); i++) {
                    pstmt.setObject(i + 1, parameters.get(i));
                }
            }
            rs = pstmt.executeQuery();
            if (rs.next())
                return rs.getObject(1);
        } catch (Exception e) {
            logger.severe(loggerLabel + " Exception " + e.toString());
        } finally {
            if (rs != null)
                try {
                    rs.close();
                } catch (SQLException e) {
                }
        }
        return defaultValue;
    }

    /**
     * @param query
     * @param parameters
     * @param numberOfRecords
     * @param con
     * @return
     */
    public List<Map<String, Object>> executeListResultQuery(String query, List<Object> parameters, int numberOfRecords, Connection con) {
        List<Map<String, Object>> listResult = new ArrayList<>();
        ResultSet rs = null;
        int count = 0;
        try (PreparedStatement pstmt = con.prepareStatement(query);) {
            if (parameters != null) {
                for (int i = 0; i < parameters.size(); i++) {
                    pstmt.setObject(i + 1, parameters.get(i));
                }
            }
            rs = pstmt.executeQuery();
            ResultSetMetaData rm = pstmt.getMetaData();
            while (rs.next()) {
                count++;
                Map<String, Object> record = new HashMap<>();
                // column have alias : use label
                for (int column = 1; column < rm.getColumnCount(); column++) {
                    record.put(rm.getColumnLabel(column).toLowerCase(), rs.getObject(column));
                }
                listResult.add(record);
                if (count >= numberOfRecords)
                    break;
            }
        }

        catch (Exception e) {
            logger.severe(loggerLabel + " Exception " + e.toString());
        } finally {
            if (rs != null)
                try {
                    rs.close();
                } catch (SQLException e) {
                }
        }
        return listResult;

    }

    /**
     * @return
     * @throws SQLException
     */
    public static Connection getConnection() throws SQLException {
        // logger.info(loggerLabel+".getDataSourceConnection() start");

        String msg = "";
        List<String> listDatasourceToCheck = new ArrayList<String>();
        for (String dataSourceString : listDataSources)
            listDatasourceToCheck.add(dataSourceString);

        for (String dataSourceString : listDatasourceToCheck) {
            logger.info(loggerLabel + ".getDataSourceConnection() check[" + dataSourceString + "]");
            try {
                final Context ctx = new InitialContext();
                final DataSource dataSource = (DataSource) ctx.lookup(dataSourceString);
                logger.info(loggerLabel + ".getDataSourceConnection() [" + dataSourceString + "] isOk");
                return dataSource.getConnection();

            } catch (NamingException e) {
                logger.info(
                        loggerLabel + ".getDataSourceConnection() error[" + dataSourceString + "] : " + e.toString());
                msg += "DataSource[" + dataSourceString + "] : error " + e.toString() + ";";
            }
        }
        logger.severe(loggerLabel + ".getDataSourceConnection: Can't found a datasource : " + msg);
        return null;
    }

    /**
     * @param o
     * @return
     */
    private Long getLong(Object o) {
        if (o == null)
            return null;
        if (o instanceof Long)
            return (Long) o;
        if (o instanceof Integer)
            return ((Integer) o).longValue();
        return Long.parseLong(o.toString());
    }
}
