package org.bonitasoft.grumman.message;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.logging.Logger;

import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import javax.sql.DataSource;

import org.bonitasoft.engine.api.ProcessAPI;
import org.bonitasoft.engine.bpm.flownode.ActivityDefinition;
import org.bonitasoft.engine.bpm.flownode.CatchEventDefinition;
import org.bonitasoft.engine.bpm.flownode.CatchMessageEventTriggerDefinition;
import org.bonitasoft.engine.bpm.flownode.FlowElementContainerDefinition;
import org.bonitasoft.engine.bpm.flownode.ReceiveTaskDefinition;
import org.bonitasoft.engine.bpm.process.DesignProcessDefinition;
import org.bonitasoft.engine.bpm.process.ProcessDefinitionNotFoundException;
import org.bonitasoft.engine.bpm.process.SubProcessDefinition;
import org.bonitasoft.engine.expression.Expression;
import org.bonitasoft.engine.operation.Operation;
import org.bonitasoft.grumman.message.Message.MessageKeyGroup;
import org.bonitasoft.grumman.message.Message.enumCatchEventType;
import org.bonitasoft.grumman.performance.PerformanceMesureSet;
import org.bonitasoft.grumman.performance.PerformanceMesureSet.PerformanceMesure;
import org.bonitasoft.grumman.reconciliation.ReconcilationMessage.ReconcialiationFilter;
import org.bonitasoft.grumman.reconciliation.ReconcilationMessage.ReconcialiationFilter.TYPEFILTER;
import org.bonitasoft.log.event.BEvent;
import org.bonitasoft.log.event.BEvent.Level;
import org.bonitasoft.properties.BonitaEngineConnection;
import org.bonitasoft.log.event.BEventFactory;

import lombok.Data;

public class MessagesFactory {

    private final static Logger logger = Logger.getLogger(MessagesFactory.class.getName());

    private static String loggerLabel = "MessagesFactory ##";
    private static String loggerExceptionLabel = " Exception ";

    private final static BEvent eventSqlQuery = new BEvent(MessagesFactory.class.getName(), 1, Level.ERROR, "SQL Query error", "Error during a SQL Query", "No value available", "check exception");
    private final static BEvent eventLoadContentMessage = new BEvent(MessagesFactory.class.getName(), 2, Level.ERROR, "Load Content Message error", "Error during the load of content of message", "No content available", "check exception");
    private final static BEvent eventNoProcessDefinitionFound = new BEvent(MessagesFactory.class.getName(), 3, Level.APPLICATIONERROR, "No process defintion found", "The process definition is missing. The process was deleted. Use the Purge Query to clean the database", "No design available", "check the process name");
    private final static BEvent eventNoTriggerDefinitionFound = new BEvent(MessagesFactory.class.getName(), 4, Level.APPLICATIONERROR, "No Trigger Definition found", "The process change: the flownode registered in the message does not exist in the process definition. Use the Purge Query to clean the database", "No design available", "check the process name");
    private final static BEvent eventListReconciliation = new BEvent(MessagesFactory.class.getName(), 5, Level.ERROR, "Error loading Reconciliation list", "Reconcialiation list can't be loaded", "No detection", "check Exception");
    private final static BEvent eventUpdatePurgeMessage = new BEvent(MessagesFactory.class.getName(), 6, Level.ERROR, "Purge message instance", "Error during purge message", "Purge is not done", "check Exception");
    private final static BEvent eventErrorLoadingDesign = new BEvent(MessagesFactory.class.getName(), 7, Level.ERROR, "Error during loading the design", "The process design can't be load", "No design will be available", "Check the process");

    /* -------------------------------------------------------------------- */
    /*                                                                      */
    /* getSynthesis */
    /*                                                                      */
    /* -------------------------------------------------------------------- */
    protected final static String SQLQUERY_COUNT_WAITINGEVENT = "select count(*) as nbevents from waiting_event where tenantid=?";
    protected final static String SQLQUERY_COUNT_MESSAGEINSTANCE = "select count(*) as nbinstances from message_instance where tenantid=?";
    protected final static String SQLQUERY_COUNT_RECONCILIATION = "select distinct count(*) " +
            "from " +
            "waiting_event w, " +
            "message_instance m " +
            "where " +
            " m.locked = 0 " +
            "AND w.locked = 0 " +
            "AND w.active = 1 " +
            // "AND f.progress = 0 " + SubProcessEvent : progress=1 even if it is not process it
            "and w.processname = m.targetprocess " +
            "and w.flownodename = m.targetflownode " +
            "and w.CORRELATION1 = m.CORRELATION1 " +
            "and w.CORRELATION2 = m.CORRELATION2 " +
            "and w.CORRELATION3 = m.CORRELATION3 " +
            "and w.CORRELATION4 = m.CORRELATION4 " +
            "and w.CORRELATION5 = m.CORRELATION5 " +
            " and w.eventtype != 'START_EVENT'";

    protected final static String SQLQUERY_RECONCILIATION_BASE = "select distinct w.eventtype as w_eventtype, " +
            " w.eventtype as w_eventtype, m.id as m_id, w.id as w_id," +
            
            " fi.lastupdatedate as w_lastupdatedate, " +
            
            " w.rootprocessinstanceid as w_rootprocessintanceid, w.parentprocessinstanceid as w_parentprocessinstanceid, " +
            " w.messagename as w_messagename, w.flownodename as w_flownodename, " +
            " w.processname as w_processname, w.processdefinitionid as w_processdefinitionid, " +
            " w.flownodeinstanceid as w_flownodeinstanceid," +
            
            " w.locked as w_locked, w.active as w_active, w.progress as w_progress,   " +
            
            " pdroot.name as w_rootprocessname, pdroot.version as w_rootversion, pdroot.processid as w_rootprocessdefinitionid,  " +
            
            " pdmessage.name as w_messageprocessname, pdmessage.version as w_messageversion, pdmessage.processid as w_messageprocessdefinitionid, " +
            
            " w.CORRELATION1 as w_correlation1, w.CORRELATION2 as w_correlation2, w.CORRELATION3 as w_correlation3, w.CORRELATION4 as w_correlation4, w.CORRELATION5 as w_correlation5,  " +
            
            " m.locked as m_locked, m.handled as m_handled " +
            
            " from waiting_event w " +
            " left join flownode_instance fi on (w.flownodeinstanceid = fi.id and fi.tenantid = w.tenantid) " +
            " left join process_instance piroot on (piroot.id = w.rootprocessinstanceid and piroot.tenantid = w.tenantid) " +
            " left join process_definition pdroot on (piroot.processdefinitionid = pdroot.processid and pdroot.tenantid = w.tenantid ) " +
            " left join process_instance pimessage on (pimessage.id = w.parentprocessinstanceid and pi.message = w.tenantid ) " +
            // link the pdMessage with the w.processfinitionid (not the pi, which not exist for a startevent
            " left join process_definition pdmessage on (w.processdefinitionid = pdmessage.processid and pdmessage.tenantid = w.tenantid ), " +
            // " left join process_definition pdmessage on (pimessage.processdefinitionid = pdmessage.processid), " +
            " message_instance m " +
            " where m.locked = 0 AND w.locked = 0 and w.active = 1" +
            // " AND w.progress = 0 " // sub process event, progress=1 even if it failed
            " and w.eventtype != 'START_EVENT' " + // Bonita keep the START_EVENT in waiting event all time !
            " and w.processname = m.targetprocess " +
            " and w.flownodename = m.targetflownode" +
            " and w.CORRELATION1 = m.CORRELATION1 " +
            " and w.CORRELATION2 = m.CORRELATION2 " +
            " and w.CORRELATION3 = m.CORRELATION3 " +
            " and w.CORRELATION4 = m.CORRELATION4 " +
            " and w.CORRELATION5 = m.CORRELATION5 " + 
            " and w.tenantid = m.tenantid " +
            " and w.tenantid = ? ";

    protected final static String SQLQUERY_RECONCILIATION_FILTERKEY = " and w.id in (?)";
    protected final static String SQLQUERY_RECONCILIATION_ORDER = " order by w_lastupdatedate";

    protected final static String SQLQUERY_LOADMESSAGE_CONTENT = "select * from data_instance where containertype='MESSAGE_INSTANCE' and containerid=? and tenantid=?";

    protected final static String SQLQUERY_CORRELATION_WAITINGEVENT = "select count(id) " +
            " from waiting_event w " +
            " where " +
            " w.processdefinitionid = (select processid from process_definition where name=? and version=?) " +
            " and w.flownodename = ? " +
            " and w.CORRELATION1 = ? " +
            " and w.CORRELATION2 = ? " +
            " and w.CORRELATION3 = ? " +
            " and w.CORRELATION4 = ? " +
            " and w.CORRELATION5 = ? " +
            " and w.tenantid = ? ";
    protected final static String SQLQUERY_COUNT_CORRELATION_MESSAGEINSTANCE = "select count(id) " +
            " from message_instance m " +
            " where " +
            " m.targetprocess=? " +
            " and m.targetflownode = ? " +
            " and m.CORRELATION1 = ? " +
            " and m.CORRELATION2 = ? " +
            " and m.CORRELATION3 = ? " +
            " and m.CORRELATION4 = ? " +
            " and m.CORRELATION5 = ? " +
            " and m.tenantid = ? ";
    protected final static String SQLQUERY_CORRELATION_MESSAGEINSTANCE = "select * " +
            " from message_instance m " +
            " where " +
            " m.targetprocess=? " +
            " and m.targetflownode = ? " +
            " and m.CORRELATION1 = ? " +
            " and m.CORRELATION2 = ? " +
            " and m.CORRELATION3 = ? " +
            " and m.CORRELATION4 = ? " +
            " and m.CORRELATION5 = ? "+
            " and m.tenantid = ? ";

    protected final static String SQLUPDATE_PURGE_MESSAGEDATA = "delete data_instance where containertype='MESSAGE_INSTANCE' and containerid=? and tenantid=?";
    protected final static String SQLUPDATE_PURGE_MESSAGE = "delete message_instance where id=? and tenantid=?";

    /* -------------------------------------------------------------------- */
    /*                                                                      */
    /* Main query to monitoring the database healthy */
    /*                                                                      */
    /* -------------------------------------------------------------------- */

    /*
     * Message instance with no valid process
     * -1 : send by the API
     */
    public final static String SQLQUERY_COUNT_MESSAGESNOMOREPROCESS = "select count(*) FROM message_instance mi WHERE NOT EXISTS (SELECT 1 FROM process_definition pd WHERE pd.name = mi.targetprocess)";
    public final static String SQLUPDATE_PURGE_MESSAGESNOMOREPROCESS = "DELETE FROM message_instance mi WHERE NOT EXISTS (SELECT 1 FROM process_definition pd WHERE pd.name = mi.targetprocess)";

    /*
     * Waiting event with no valid process
     */
    public final static String SQLQUERY_COUNT_WAITINGEVENTNOMOREPROCESS = "select count(*) FROM waiting_event we WHERE NOT EXISTS (SELECT 1 FROM process_definition pd WHERE pd.processid = we.processdefinitionid)";
    public final static String SQLUPDATE_PURGE_WAITINGEVENTNOMOREPROCESS = "DELETE FROM waiting_event we WHERE NOT EXISTS (SELECT 1 FROM process_definition pd WHERE pd.processid = we.processdefinitionid)";

    /*
     * >waiting_events from process instances that have already been archived:
     * --------------------------------------------------------------------------------------------------------------------
     * (Not likely to be any such waiting_event, but just in case)
     */
    public final static String SQLQUERY_COUNT_WAITINGEVENTNOMOREPROCESSINSTANCE = "SELECT count(*) FROM waiting_event we WHERE NOT EXISTS (SELECT 1 FROM process_instance pi WHERE pi.id = we.rootprocessinstanceid) and eventtype != 'START_EVENT'";
    public final static String SQLUPDATE_PURGE_WAITINGEVENTNOMOREPROCESSINSTANCE = "DELETE FROM waiting_event we WHERE NOT EXISTS (SELECT 1 FROM process_instance pi WHERE pi.id = we.rootprocessinstanceid)  and eventtype != 'START_EVENT'";

    /*
     * Data attached to message, but the message does not exist
     */
    public final static String SQLQUERY_COUNT_DATAWITHOUTMESSAGE = "SELECT count(*) FROM data_instance di WHERE di.containertype = 'MESSAGE_INSTANCE' AND NOT EXISTS (SELECT 1 FROM message_instance mi WHERE di.containerid = mi.id)";
    public final static String SQLUPDATE_PURGE_DATAWITHOUTMESSAGE = "DELETE FROM data_instance di WHERE di.containertype = 'MESSAGE_INSTANCE' AND NOT EXISTS (SELECT 1 FROM message_instance mi WHERE di.containerid = mi.id)";

    private static final String CSTJSON_PERFORMANCEMESURE = "performancemesure";

    private static final String CSTJSON_PERFORMANCEMESURETOTAL = "total";

    /* -------------------------------------------------------------------- */
    /*                                                                      */
    /* Detect duplicate message */
    /*                                                                      */
    /* -------------------------------------------------------------------- */
    public final static String SQLQUERY_SEARCHDUPLICATEMESSAGEINSTANCE = " SELECT a.id, b.id as duplicate_id, a.* "
            + " from message_instance a, message_instance b "
            + " where a.id < b.id "
            + " and a.id = (select min(c.id) "
            + " from message_instance c "
            + " where c.targetprocess = a.targetprocess"
            + " and c.targetflownode = a.targetflownode"
            + " and c.CORRELATION1 = a.CORRELATION1"
            + " and c.CORRELATION2 = a.CORRELATION2"
            + " and c.CORRELATION3 = a.CORRELATION3"
            + " and c.CORRELATION4 = a.CORRELATION4"
            + " and c.CORRELATION5 = a.CORRELATION5)"
            + " and a.targetprocess = b.targetprocess "
            + " and a.targetflownode = b.targetflownode "
            + " and NOT (a.CORRELATION1 = 'NONE' and a.CORRELATION2 = 'NONE' and a.CORRELATION3 = 'NONE' and a.CORRELATION4 != 'NONE' or a.CORRELATION5 != 'NONE')"
            + " and a.CORRELATION1 = b.CORRELATION1 "
            + " and a.CORRELATION2 = b.CORRELATION2 "
            + " and a.CORRELATION3 = b.CORRELATION3 "
            + " and a.CORRELATION4 = b.CORRELATION4 "
            + " and a.CORRELATION5 = b.CORRELATION5 "
            + " order by a.id, a.targetprocess, a.targetflownode, a.CORRELATION1, a.CORRELATION2, a.CORRELATION3, a.CORRELATION4, a.CORRELATION5, b.id";

    /** purge all duplicate for the given id BUT not the given id */
    public final static String SQLQUERY_PURGEDUPLICATEMESSAGEINSTANCE = "DELETE FROM message_instance c WHERE c.id in"
            + " (select b.id from message_instance a, message_instance b"
            + "  where a.id = ?"
            + " and b.id > a.id   "
            + " and a.targetprocess = b.targetprocess"
            + " and a.targetflownode = b.targetflownode"
            + " and a.CORRELATION1 = b.CORRELATION1 "
            + " and a.CORRELATION2 = b.CORRELATION2 "
            + " and a.CORRELATION3 = b.CORRELATION3 "
            + " and a.CORRELATION4 = b.CORRELATION4 "
            + " and a.CORRELATION5 = b.CORRELATION5 "
            + ")";

    private long tenantId;
    public MessagesFactory( long tenantId) {
        this.tenantId = tenantId;
    }
    
    public @Data class SynthesisOnMessage {

        private long nbWaitingEvent;
        private long nbMessageEvent;
        private long nbReconciliation;
        private List<BEvent> listEvents = new ArrayList<>();
        private PerformanceMesureSet performanceMesure = new PerformanceMesureSet();

        public Map<String, Object> getMap() {
            Map<String, Object> result = new HashMap<>();
            result.put("nbwaitingevent", nbWaitingEvent);
            result.put("nbmessageevent", nbMessageEvent);
            result.put("nbreconciliation", nbReconciliation);
            if (!listEvents.isEmpty())
                result.put("listevents", BEventFactory.getSyntheticHtml(listEvents));

            result.put(CSTJSON_PERFORMANCEMESURE, performanceMesure.getMap());

            return result;
        }

    }

    public SynthesisOnMessage getSynthesis() {
        SynthesisOnMessage synthesisOnMessage = new SynthesisOnMessage();
        PerformanceMesure perf = synthesisOnMessage.performanceMesure.getMesure(CSTJSON_PERFORMANCEMESURETOTAL);
        perf.start();

        try (Connection con = getConnection();) {
            ResultQuery result;

            List<Object> parameters= new ArrayList<>();
            parameters.add( tenantId );
            
            result = executeOneResultQuery("waitingevent", SQLQUERY_COUNT_WAITINGEVENT, parameters, 0L, con);
            synthesisOnMessage.listEvents.addAll(result.listEvents);
            synthesisOnMessage.nbWaitingEvent = getLong(result.oneResult, 0L);

            result = executeOneResultQuery("messageinstance", SQLQUERY_COUNT_MESSAGEINSTANCE, null, 0L, con);
            synthesisOnMessage.listEvents.addAll(result.listEvents);
            synthesisOnMessage.nbMessageEvent = getLong(result.oneResult, 0L);

            /*
             * result = executeOneResultQuery("reconciliation", SQLQUERY_COUNT_RECONCILIATION, null, 0L, con);
             * synthesisOnMessage.listEvents.addAll(result.listEvents);
             * synthesisOnMessage.nbReconciliation = getLong(result.oneResult, 0L);
             */

            result = executeListResultQuery("countreconciliation", SQLQUERY_RECONCILIATION_BASE + SQLQUERY_RECONCILIATION_ORDER, null, 1, "w_id", con);
            synthesisOnMessage.nbReconciliation = result.numberOfDiffAttributes;

        } catch (Exception e) {
            synthesisOnMessage.listEvents.add(new BEvent(eventSqlQuery, e, ""));
        }
        perf.stop();
        return synthesisOnMessage;
    }

    /* -------------------------------------------------------------------- */
    /*                                                                      */
    /* Message */
    /*                                                                      */
    /* -------------------------------------------------------------------- */

    /* -------------------------------------------------------------------- */
    /*                                                                      */
    /* operations on message */
    /*                                                                      */
    /* -------------------------------------------------------------------- */
    public @Data class ResultOperation {

        private List<Message> listMessages = new ArrayList<>();
        private List<BEvent> listEvents = new ArrayList<>();
        private int numberOfMessages;

        private PerformanceMesureSet performanceMesure = new PerformanceMesureSet();

    }

    public ResultOperation getListReconcilationMessages(ReconcialiationFilter reconciliationFilter) {
        ResultOperation resultOperation = new ResultOperation();
        PerformanceMesure perf = resultOperation.performanceMesure.getMesure("getlistreconciliation");
        perf.start();

        String sqlQuery = "";
        try (Connection con = getConnection();) {

            ResultQuery resultQuery;
            List<Object> parameters = new ArrayList<>();
            parameters.add( tenantId );
            if (reconciliationFilter.typeFilter == TYPEFILTER.MAXMESSAGES) {
                sqlQuery = SQLQUERY_RECONCILIATION_BASE + SQLQUERY_RECONCILIATION_ORDER;
                resultQuery = executeListResultQuery("reconciliation", SQLQUERY_RECONCILIATION_BASE + SQLQUERY_RECONCILIATION_ORDER, parameters, reconciliationFilter.numberOfMessages, "w_id", con);
                resultOperation.numberOfMessages = resultQuery.numberOfDiffAttributes;

            } else if (reconciliationFilter.typeFilter == TYPEFILTER.MESSAGEKEY) {
                StringBuilder filter = new StringBuilder();
                for (int i = reconciliationFilter.fromIndex; i < reconciliationFilter.toIndex; i++) {
                    if (i > reconciliationFilter.fromIndex)
                        filter.append(",");

                    filter.append("? ");
                    parameters.add(reconciliationFilter.messagesList.getListKeys().get(i));
                }
                sqlQuery = SQLQUERY_RECONCILIATION_BASE + SQLQUERY_RECONCILIATION_FILTERKEY + SQLQUERY_RECONCILIATION_ORDER;
                resultQuery = executeListResultQuery("reconciliation", SQLQUERY_RECONCILIATION_BASE + SQLQUERY_RECONCILIATION_FILTERKEY + SQLQUERY_RECONCILIATION_ORDER, Arrays.asList(filter), reconciliationFilter.numberOfMessages, "w_id", con);
                resultOperation.numberOfMessages = resultQuery.numberOfDiffAttributes;

            } else if (reconciliationFilter.typeFilter == TYPEFILTER.GROUPKEY) {
                StringBuilder filter = new StringBuilder();
                filter.append(" and (");
                for (int i = reconciliationFilter.fromIndex; i < reconciliationFilter.toIndex; i++) {
                    if (i > reconciliationFilter.fromIndex)
                        filter.append(" or ");
                    filter.append("( w.processdefinitionid =(select pd.processid from process_definition pd where pd.name=? and pd.version=? and tenantid=w.tenantid) and w.flownodename=? )");
                    MessageKeyGroup messageKeyGroup = MessageKeyGroup.getInstanceFromKey(reconciliationFilter.messagesList.getListKeysGroups().get(i));
                    parameters.add(messageKeyGroup.processName);
                    parameters.add(messageKeyGroup.processVersion);
                    parameters.add(messageKeyGroup.flowNodeName);
                }
                filter.append(")");

                sqlQuery = SQLQUERY_RECONCILIATION_BASE + filter + SQLQUERY_RECONCILIATION_ORDER;
                resultQuery = executeListResultQuery("reconciliation", SQLQUERY_RECONCILIATION_BASE + filter + SQLQUERY_RECONCILIATION_ORDER, parameters, reconciliationFilter.numberOfMessages, "w_id", con);
                resultOperation.numberOfMessages = resultQuery.numberOfDiffAttributes;

            } else {
                // unknow filter
                return resultOperation;
            }
            resultOperation.performanceMesure.add(resultQuery.performanceMesure);

            resultOperation.listEvents.addAll(resultQuery.listEvents);
            for (Map<String, Object> record : resultQuery.getListResult()) {
                Message message = new Message();
                for (Entry<String, Object> entry : record.entrySet()) {
                    if (entry.getKey().startsWith("w_"))
                        message.getWaitingEvent().put(entry.getKey().substring(2), entry.getValue());
                    if (entry.getKey().startsWith("m_"))
                        message.getMessageInstance().put(entry.getKey().substring(2), entry.getValue());
                }
                message.isMessageWithCorrelation = true;
                message.setMessageName((String) record.get("w_messagename"));
                message.setRootProcessInstanceId(getLong(record.get("w_rootprocessintanceid"), null));
                message.setProcessInstanceId(getLong(record.get("w_parentprocessinstanceid"), null));

                message.setTargetProcessName((String) record.get("w_processname"));
                message.setProcessName((String) record.get("w_rootprocessname"));

                message.setTargetFlowNodeName((String) record.get("w_flownodename"));
                message.setFlowNodeName((String) record.get("w_flownodename"));

                message.setCurrentProcessVersion((String) record.get("w_messageversion"));
                message.setProcessDefinitionId(getLong(record.get("w_messageprocessdefinitionid"), null));

                message.setRootProcessName((String) record.get("w_rootprocessname"));
                message.setRootProcessVersion((String) record.get("w_rootversion"));
                message.setRootProcessDefinitionId(getLong(record.get("w_rootprocessdefinitionid"), null));

                message.setFlowNodeInstanceId(getLong(record.get("w_flownodeinstanceid"), null));
                message.setDateWaitingEvent(getLong(record.get("w_lastupdatedate"), null));

                message.setMessageId(getLong(record.get("m_id"), null));
                message.setWaitingId(getLong(record.get("w_id"), null));
                resultOperation.listMessages.add(message);
            }

            if (reconciliationFilter.reduceCrossJoint) {
                PerformanceMesure perfCrossJoint = resultOperation.performanceMesure.getMesure("reducecrossjoint");
                perfCrossJoint.start();
                resultOperation.listMessages = reduceCrossJointMessages(resultOperation.listMessages);
                perfCrossJoint.stop();
            }
        } catch (Exception e) {
            logger.severe(loggerLabel + loggerExceptionLabel + e.toString());

            StringWriter sw = new StringWriter();
            e.printStackTrace(new PrintWriter(sw));
            String exceptionDetails = sw.toString();
            resultOperation.listEvents.add(new BEvent(eventListReconciliation, e, "SqlQuery[" + sqlQuery + "] - " + e.getMessage() + " at " + exceptionDetails));
        }
        perf.stop();
        return resultOperation;
    }

    // we don't manage CLOB and BLOB
    private static String[] listColumDatainstance = { "intvalue", "longvalue", "shorttextvalue", "booleanvalue", "doublevalue", "floatvalue" };

    /**
     * Load the message Content
     * 
     * @param message
     * @param loadValues
     * @return
     */
    public List<BEvent> loadMessageContentData(Message message, boolean loadValues) {
        List<BEvent> listEvents = new ArrayList<>();

        try (Connection con = getConnection();) {
            List<Object> parameters = new ArrayList();
            parameters.add( message.messageId);
            parameters.add( tenantId );
            ResultQuery resultQuery = executeListResultQuery("messagecontentdata", SQLQUERY_LOADMESSAGE_CONTENT, parameters, 100, null, con);
            listEvents.addAll(resultQuery.listEvents);
            for (Map<String, Object> record : resultQuery.getListResult()) {
                Object value = null;
                if (loadValues) {
                    for (String columName : listColumDatainstance) {
                        if (record.get(columName) != null)
                            value = record.get(columName);
                    }
                }
                message.messageInstanceVariables.put((String) record.get("name"), value);
            }
        } catch (Exception e) {
            logger.severe(loggerLabel + loggerExceptionLabel + e.toString());
            listEvents.add(new BEvent(eventLoadContentMessage, e, ""));
        }
        return listEvents;
    }

    /**
     * @param processDefinitionId
     * @param taskName
     * @return
     */
    public List<BEvent> loadDesignContentData(Message message, ProcessAPI processAPI, Map<Long, DesignProcessDefinition> cacheDesign) {
        List<BEvent> listEvents = new ArrayList<>();
        if (message.getProcessDefinitionId() == null || message.getTargetFlowNodeName() == null)
            return listEvents;
        try {
            StringBuilder detailExecution = new StringBuilder();

            DesignProcessDefinition designProcessAPI = cacheDesign.get(message.getProcessDefinitionId());
            if (designProcessAPI == null) {
                try {
                    designProcessAPI = processAPI.getDesignProcessDefinition(message.getProcessDefinitionId());
                } catch (ProcessDefinitionNotFoundException e) {
                    logger.severe(loggerLabel + loggerExceptionLabel + e.toString());
                    listEvents.add(new BEvent(eventNoProcessDefinitionFound, "ProcessName[" + message.getTargetProcessName() + "] Version[" + message.getCurrentProcessVersion() + "]"));
                    message.explanationDetail.append(eventNoProcessDefinitionFound.toString());
                    return listEvents;
                }
                catch(Exception e) {
                    // error during the design loading
                    logger.severe(loggerLabel + loggerExceptionLabel + e.toString());
                    listEvents.add(new BEvent(eventErrorLoadingDesign, "ProcessName[" + message.getTargetProcessName() + "] Version[" + message.getCurrentProcessVersion() + "]"));
                    message.explanationDetail.append(eventErrorLoadingDesign.toString());
                    return listEvents;
                }
                cacheDesign.put(message.getProcessDefinitionId(), designProcessAPI);
            }

            FlowElementContainerDefinition flowElementContainer = designProcessAPI.getFlowElementContainer();
            // maybe an activity or a intermediate catch event, Boundary event, Activity
            ReceiveTaskDefinition activityMessage = null;

            message.setDesignContentFound( false );
            // avoid the class cast exception
            List<CatchEventDefinition> listCatchsEvent = new ArrayList<>();

            detailExecution.append("Search[" + message.getTargetFlowNodeName() + "] activities:");
            List<ActivityDefinition> listActivities = flowElementContainer.getActivities();
            for (ActivityDefinition activity : listActivities) {
                detailExecution.append("[" + activity.getName() + "]");
                if (activity.getName().equals(message.getTargetFlowNodeName()) && activity instanceof ReceiveTaskDefinition) {
                    activityMessage = (ReceiveTaskDefinition) activity;

                    message.setCorrelations(activityMessage.getTrigger().getCorrelations());
                    message.addListOperations(activityMessage.getTrigger().getOperations());
                    message.catchEventType = enumCatchEventType.TASKMESSAGE;
                    message.setDesignContentFound( true );

                    detailExecution.append("*MATCH*");
                    break;
                }

                listCatchsEvent.clear();
                listCatchsEvent.addAll(activity.getBoundaryEventDefinitions());
                detailExecution.append(detectCatchEvent(listCatchsEvent, message, enumCatchEventType.BOUNDARYEVENT));

                if (activity instanceof SubProcessDefinition) {
                    @SuppressWarnings("deprecation")
                    FlowElementContainerDefinition flowElementSubprocessContainer = ((SubProcessDefinition) activity).getSubProcessContainer();
                    listCatchsEvent.clear();
                    listCatchsEvent.addAll(flowElementSubprocessContainer.getStartEvents());
                    detailExecution.append(detectCatchEvent(listCatchsEvent, message, enumCatchEventType.SUBPROCESSEVENT));

                }
            } // end loop activity
            detailExecution.append("CatchEvent:");

            listCatchsEvent.clear();
            listCatchsEvent.addAll(flowElementContainer.getIntermediateCatchEvents());
            detailExecution.append(detectCatchEvent(listCatchsEvent, message, enumCatchEventType.CATCHMESSAGEEVENT));

            // check events

            

            if (!message.isDesignContentFound() ) {
                message.explanationDetail = detailExecution;
                return listEvents;
            }

            // ---- we have a definition
            message.designContent.clear();

            // now analyse operation
            for (Operation operation : message.listOperations) {
                Expression rightOperand = operation.getRightOperand();
                if (rightOperand!=null && "TYPE_VARIABLE".equals(rightOperand.getExpressionType()) && rightOperand.getContent() !=null)
                    message.designContent.add(rightOperand.getContent());
            }

            return listEvents;

       } catch (Exception e) {

           StringWriter sw = new StringWriter();
            e.printStackTrace(new PrintWriter(sw));
            String exceptionDetails = sw.toString();

            logger.severe(loggerLabel + loggerExceptionLabel + e.toString()+" at "+exceptionDetails);
            listEvents.add(new BEvent(eventNoTriggerDefinitionFound, "ProcessName[" + message.getTargetProcessName() + "] Version[" + message.getCurrentProcessVersion() + "] FlowNodeName[" + message.getTargetFlowNodeName() + "]"));
            message.explanationDetail.append("Error "+e.getMessage()+" at "+exceptionDetails);
        }

        return listEvents;

    }

    /**
     * detect the expected catchEvent in the list provided.
     * 
     * @param listCatchsEvent
     * @param message
     * @param catchEventType
     * @return
     */
    private String detectCatchEvent(List<CatchEventDefinition> listCatchsEvent, Message message, enumCatchEventType catchEventType) {
        StringBuilder detailExecution = new StringBuilder();
        for (CatchEventDefinition catchEvent : listCatchsEvent) {
            detailExecution.append("[" + catchEvent.getName() + "]");

            if (catchEvent.getName().equals(message.getTargetFlowNodeName())) {
                CatchMessageEventTriggerDefinition catchDefinition = getCatchMessageEventTriggerFromEvent(catchEvent);
                if (catchDefinition != null) {
                    message.setCorrelations(catchDefinition.getCorrelations());
                    message.addListOperations(catchDefinition.getOperations());
                    message.catchEventType = catchEventType;
                    message.setDesignContentFound( true );
                    detailExecution.append("*MATCH*");
                    break;
                }
            }
        }
        return detailExecution.toString();
    }

    /**
     * get all operations attached to a catchEventDefinition
     * 
     * @param catchEventDefinition
     * @return
     */
    private CatchMessageEventTriggerDefinition getCatchMessageEventTriggerFromEvent(CatchEventDefinition catchEventDefinition) {

        try {
            List<CatchMessageEventTriggerDefinition> listEventsTrigger = catchEventDefinition.getMessageEventTriggerDefinitions();
            if (! listEventsTrigger.isEmpty())
                return listEventsTrigger.get(0);

        } catch (Exception e) {
            // should never arrived
        }
        return null;
    }

    /**
     * Load, a a correlation key, number of waiting event and message
     * 
     * @param message
     */
    public List<BEvent> loadRelativeInformations(Message message) {
        List<BEvent> listEvents = new ArrayList<>();

        try (Connection con = getConnection();) {

            // search, for the correlation, 
            // how many waiting_event are present
            // how many message_instance are present
            List<Object> parameters = new ArrayList<>();
            parameters.add(message.getTargetProcessName());
            parameters.add(message.getCurrentProcessVersion());
            parameters.add(message.getTargetFlowNodeName());
            for (int i = 0; i < 5; i++)
                parameters.add(message.getValueCorrelation(i, true));
            parameters.add( tenantId );
            
            ResultQuery resultQuery = executeOneResultQuery("correlationwaitingevent", SQLQUERY_CORRELATION_WAITINGEVENT, parameters, 0L, con);
            message.nbWaitingEvent = getLong(resultQuery.oneResult, 0L);
            listEvents.addAll(resultQuery.listEvents);

            parameters.clear();
            parameters.add(message.getTargetProcessName());
            parameters.add(message.getTargetFlowNodeName());
            for (int i = 0; i < 5; i++)
                parameters.add(message.getValueCorrelation(i, true));
            parameters.add( tenantId );

            resultQuery = executeListResultQuery("correlationmessageinstance", SQLQUERY_CORRELATION_MESSAGEINSTANCE, parameters, 10000, null, con);
            listEvents.addAll(resultQuery.listEvents);

            for (Map<String, Object> record : resultQuery.getListResult()) {
                message.listIdMessageInstanceRelative.add(getLong(record.get("id"), -1L));
            }
            message.nbMessageInstance = message.listIdMessageInstanceRelative.size();
        } catch (Exception e) {
            logger.severe(loggerLabel + loggerExceptionLabel + e.toString());
            StringWriter sw = new StringWriter();
            e.printStackTrace(new PrintWriter(sw));
            String exceptionDetails = sw.toString();

            listEvents.add(new BEvent(eventSqlQuery, e, " " + e.getMessage() + " at " + exceptionDetails));
        }

        return listEvents;

    }

    /**
     * Purge the message instance attached to the message
     * 
     * @param message
     * @param nbRecordToPurge
     * @return
     */

    public @Data class ResultPurge {

        private List<BEvent> listEvents = new ArrayList<>();
        private int nbDatasRowDeleted = 0;
        private int nbMessagesRowDeleted = 0;
        private List<Long> listIdMessageInstancePurged = new ArrayList<>();
        private PerformanceMesureSet performanceMesure = new PerformanceMesureSet();
    }

    /**
     * purge a list of Message Id
     * 
     * @param message
     * @param purgeAll
     * @param nbRecordToPurge
     * @return
     */
    public ResultPurge purgeMessageInstance(List<Long> listMessagesId, boolean purgeAll, int nbRecordToPurge) {
        ResultPurge resultPurge = new ResultPurge();
        PerformanceMesure perf = resultPurge.performanceMesure.getMesure("purgemessageinstance");
        perf.start();

        int count = 0;
        String query = "";
        Connection connection = null;
        try (Connection con = getConnection();) {
            connection = con;
            con.setAutoCommit(false);
            List<Object> parameters = new ArrayList<>();
            // use the 
            for (Long id : listMessagesId) {
                // stop if we reach the number of purge
                if (!purgeAll && count >= nbRecordToPurge)
                    break;
                count++;

                parameters.clear();
                parameters.add( id );
                parameters.add( tenantId );

                query = SQLUPDATE_PURGE_MESSAGEDATA;
                ResultQuery resultQuery = executeUpdateQuery("purgemessagedata", query, parameters, con, false);
                resultPurge.nbDatasRowDeleted += resultQuery.numberOfRows;
                resultPurge.listEvents.addAll(resultQuery.listEvents);

                query = SQLUPDATE_PURGE_MESSAGE;
                resultQuery = executeUpdateQuery("purgemessage", query, parameters, con, false);
                resultPurge.nbMessagesRowDeleted += resultQuery.numberOfRows;
                resultPurge.listEvents.addAll(resultQuery.listEvents);
                resultPurge.listIdMessageInstancePurged.add(id);
            }
            con.commit();
        } catch (Exception e) {
            logger.severe(loggerLabel + loggerExceptionLabel + e.toString());
            resultPurge.listEvents.add(new BEvent(eventUpdatePurgeMessage, e, "query[" + query + "] " + e.getMessage()));
            if (connection != null)
                try {
                    connection.rollback();
                } catch (SQLException e1) {
                }
        }
        perf.stop();

        return resultPurge;
    }

    /* -------------------------------------------------------------------- */
    /*                                                                      */
    /* getConnection */
    /*                                                                      */
    /* -------------------------------------------------------------------- */
    private static List<String> listDataSources = Arrays.asList("java:/comp/env/bonitaSequenceManagerDS",
            "java:jboss/datasources/bonitaSequenceManagerDS");

    /**
     * ResultQuery
     */
    public @Data class ResultQuery {

        private List<Map<String, Object>> listResult = new ArrayList<>();
        private Object oneResult;
        private List<BEvent> listEvents = new ArrayList<>();

        private int numberOfRows = 0;
        private int numberOfDiffAttributes = 0;

        private PerformanceMesureSet performanceMesure = new PerformanceMesureSet();

    }

    /**
     * @param query
     * @param parameters
     * @param defaultValue
     * @param con
     * @return
     */
    public ResultQuery executeOneResultQuery(String name, String query, List<Object> parameters, Object defaultValue, Connection con) {
        ResultQuery resultQuery = new ResultQuery();

        PerformanceMesure perf = resultQuery.performanceMesure.getMesure("sqlrequestOneQuery_" + name);
        perf.start();
        ResultSet rs = null;
        try (PreparedStatement pstmt = con.prepareStatement(query);) {
            if (parameters != null) {
                for (int i = 0; i < parameters.size(); i++) {
                    pstmt.setObject(i + 1, parameters.get(i));
                }
            }
            rs = pstmt.executeQuery();
            if (rs.next()) {
                resultQuery.oneResult = rs.getObject(1);
            }
            return resultQuery;
        } catch (Exception e) {
            logger.severe(loggerLabel + loggerExceptionLabel + e.toString());
            resultQuery.listEvents.add(new BEvent(eventSqlQuery, e, "query[" + query + "]"));
        } finally {
            if (rs != null)
                try {
                    rs.close();
                } catch (SQLException e) {
                }
            perf.stop();
        }
        return resultQuery;
    }

    /**
     * @param query
     * @param parameters
     * @param con
     * @return
     */
    public ResultQuery executeUpdateQuery(String name, String query, List<Object> parameters, Connection con, boolean commit) {
        ResultQuery resultQuery = new ResultQuery();
        PerformanceMesure perf = resultQuery.performanceMesure.getMesure("sqlupdate_" + name);
        perf.start();

        try (PreparedStatement pstmt = con.prepareStatement(query);) {
            if (commit)
                con.setAutoCommit(false); // default true
            if (parameters != null) {
                for (int i = 0; i < parameters.size(); i++) {
                    pstmt.setObject(i + 1, parameters.get(i));
                }
            }
            resultQuery.numberOfRows = pstmt.executeUpdate();

            if (commit)
                con.commit();
            return resultQuery;
        } catch (Exception e) {
            logger.severe(loggerLabel + loggerExceptionLabel + e.toString());
            resultQuery.listEvents.add(new BEvent(eventSqlQuery, e, "query[" + query + "]"));
            if (commit)
                try {
                    con.rollback();
                } catch (SQLException e1) {
                }

        } finally {
            perf.stop();
        }
        return resultQuery;
    }

    /**
     * @param query
     * @param parameters
     * @param numberOfRecords
     * @param con
     * @return
     */
    public ResultQuery executeListResultQuery(String name, String query, List<Object> parameters, int numberOfRecords, String diffAttribute, Connection con) {
        ResultQuery resultQuery = new ResultQuery();
        PerformanceMesure perf = resultQuery.performanceMesure.getMesure("sqlrequestlist_" + name);
        perf.start();

        Set<String> setOfDiffAttribut = new HashSet<>();
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
                if (diffAttribute != null)
                    setOfDiffAttribut.add(record.get(diffAttribute) == null ? "NULL" : record.get(diffAttribute).toString());
                if (count <= numberOfRecords)
                    resultQuery.listResult.add(record);
                // rs.last() may not run
                resultQuery.numberOfRows++;
            }
            resultQuery.numberOfDiffAttributes = setOfDiffAttribut.size();
        }

        catch (Exception e) {
            logger.severe(loggerLabel + loggerExceptionLabel + e.toString());
            resultQuery.listEvents.add(new BEvent(eventSqlQuery, e, "query[" + query + "]"));

        } finally {
            if (rs != null)
                try {
                    rs.close();
                } catch (SQLException e) {
                }
            perf.stop();

        }
        return resultQuery;

    }

    /**
     * @return
     * @throws SQLException
     */
    public static Connection getConnection() throws SQLException {
        // ".getDataSourceConnection() start"
        return BonitaEngineConnection.getConnection();
    }

    /* -------------------------------------------------------------------- */
    /*                                                                      */
    /* reduce message */
    /*                                                                      */
    /* -------------------------------------------------------------------- */

    /**
     * in there are, for a signature message, 4 waiting Event, 5 message instance, then the list contains 4*5 messages.
     * In this situation, we want to keep min(<waiting event>,<message_instance>) message.
     * If there are less message_instance, we keep this number. The correlation Message is only message with a waiting AND a message instance. That's mean there
     * are still some event to wait
     * we want to keep the "last according message".
     * example :
     * WaitingEventID
     * 12
     * 13
     * MessageEventId
     * 151
     * 152
     * 153
     * ==> Result expected : 12-151, 13-153
     * Remark 1 : result may be 13-151 and 12-153 (id order is not important)
     * Remark 2 : if there are LESS waitingEvent than messageEvent, then don't keep them
     * Remark 3 : to be sure to keep the LAST waiting event, let's first order the list by the date
     * 
     * @param listMessages
     */
    private class SignatureMessage {

        Set<String> listSnippets = new HashSet<>();
        Set<Long> listWaitingEvent = new HashSet<>();
        Set<Long> listMessageInstance = new HashSet<>();
    }

    private List<Message> reduceCrossJointMessages(List<Message> listMessages) {
        // first,order the list by the day
        sortMyMessageList(listMessages, new String[] { "waitingevent.lastupdatedate", "waitingevent.id", "messageinstance.id" });

        Map<String, SignatureMessage> setListMessageSignature = new HashMap<>();
        List<Message> listReduced = new ArrayList<>();
        for (Message message : listMessages) {

            SignatureMessage listCollectedMessage = setListMessageSignature.get(message.getSignatureMessage());
            if (listCollectedMessage == null) {
                // ok, new one
                listCollectedMessage = new SignatureMessage();
            }

            // Calculate, for the signature, the number of WAITING_EVENT and number of MESSAGE_EVENT.
            // message return all combinaison : for 3 waiting event / 4 message, there are 3*4 messages
            listCollectedMessage.listWaitingEvent.add(message.waitingId);
            listCollectedMessage.listMessageInstance.add(message.messageId);

            // is this same waiting_id OR message_Id is referenced, do not keep it. We want to keep only unique new item
            if (listCollectedMessage.listSnippets.contains("W" + message.waitingId)
                    || listCollectedMessage.listSnippets.contains("M" + message.messageId))
                continue;
            // ok, we keep that one
            listReduced.add(message);
            listCollectedMessage.listSnippets.add("W" + message.waitingId);
            listCollectedMessage.listSnippets.add("M" + message.messageId);

            setListMessageSignature.put(message.getSignatureMessage(), listCollectedMessage);
        }

        // now, review the message and complete the number of "browser" for each
        for (Message message : listReduced) {
            SignatureMessage listCollectedMessage = setListMessageSignature.get(message.getSignatureMessage());
            if (listCollectedMessage != null) {
                message.setSameSignatureNbMessageInstance(listCollectedMessage.listMessageInstance.size());
                message.setSameSignatureNbWaitingEvent(listCollectedMessage.listWaitingEvent.size());
            }

        }

        return listReduced;
    }

    /* -------------------------------------------------------------------- */
    /*                                                                      */
    /* Static tool */
    /*                                                                      */
    /* -------------------------------------------------------------------- */

    /**
     * @param o
     * @return
     */
    public static Long getLong(Object o, Long defaultValue) {
        try {
            if (o == null)
                return defaultValue;
            if (o instanceof Long)
                return (Long) o;
            if (o instanceof Integer)
                return ((Integer) o).longValue();
            return Long.parseLong(o.toString());
        } catch (Exception e) {
            return defaultValue;
        }
    }

    public static Boolean getBoolean(Object o, Boolean defaultValue) {
        try {
            if (o == null)
                return defaultValue;
            if (o instanceof Long)
                return ((Long) o).equals(1);
            if (o instanceof Integer)
                return ((Integer) o).equals(1);

            return "true".equalsIgnoreCase(o.toString());
        } catch (Exception e) {
            return defaultValue;
        }
    }

    public static void sortMyMessageList(List<Message> list, String[] attributs) {
        Collections.sort(list, new Comparator<Message>() {

            public int compare(Message s1,
                    Message s2) {
                for (String attribut : attributs) {
                    Object key1 = null;
                    Object key2 = null;

                    if (attribut.startsWith("waitingevent")) {
                        key1 = s1.getWaitingEvent().get(attribut.substring("waitingevent.".length()));
                        key2 = s2.getWaitingEvent().get(attribut.substring("waitingevent.".length()));
                    }
                    if (attribut.startsWith("messageinstance")) {
                        key1 = s1.getMessageInstance().get(attribut.substring("messageinstance.".length()));
                        key2 = s2.getMessageInstance().get(attribut.substring("messageinstance.".length()));
                    }

                    if (key1 == null)
                        return 1;
                    if (key2 == null)
                        return -1;
                    // no interface...
                    int comparaison = 0;
                    if (key1 instanceof String)
                        comparaison = ((String) key1).compareTo((String) key2);
                    else if (key1 instanceof Integer)
                        comparaison = ((Integer) key1).compareTo((Integer) key2);
                    else if (key1 instanceof Long)
                        comparaison = ((Long) key1).compareTo((Long) key2);
                    else
                        comparaison = key1.toString().compareTo(key2.toString());

                    if (comparaison != 0)
                        return comparaison;
                }
                // if we are here, that mean all attributs value are the same
                return 0;

            }
        });
    }

    public static void sortMyList(List<Map<String, Object>> list, String[] attributs) {
        Collections.sort(list, new Comparator<Map<String, Object>>() {

            public int compare(Map<String, Object> s1,
                    Map<String, Object> s2) {
                for (String attribut : attributs) {
                    Object key1 = s1.get(attribut);
                    Object key2 = s2.get(attribut);
                    if (key1 == null)
                        return 1;
                    if (key2 == null)
                        return -1;
                    // no interface...
                    int comparaison = 0;
                    if (key1 instanceof String)
                        comparaison = ((String) key1).compareTo((String) key2);
                    else if (key1 instanceof Integer)
                        comparaison = ((Integer) key1).compareTo((Integer) key2);
                    else if (key1 instanceof Long)
                        comparaison = ((Long) key1).compareTo((Long) key2);
                    else
                        comparaison = key1.toString().compareTo(key2.toString());

                    if (comparaison != 0)
                        return comparaison;
                }
                // if we are here, that mean all attributs value are the same
                return 0;

            }
        });
    }
}
