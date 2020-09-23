package org.bonitasoft.grumman.purge;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.bonitasoft.engine.api.ProcessAPI;
import org.bonitasoft.grumman.message.MessagesFactory;
import org.bonitasoft.grumman.message.MessagesFactory.ResultQuery;
import org.bonitasoft.grumman.performance.PerformanceMesureSet;
import org.bonitasoft.grumman.performance.PerformanceMesureSet.PerformanceMesure;
import org.bonitasoft.log.event.BEvent;
import org.bonitasoft.log.event.BEvent.Level;
import org.bonitasoft.log.event.BEventFactory;

public class PurgeTablesMessage {

    private final static BEvent eventSqlQueryError = new BEvent(MessagesFactory.class.getName(), 1, Level.ERROR, "Purge query error", "A Purge Sql query has an error", "No result for the query", "check exception");

    private static final String CSTJSON_PERFORMANCEMESURE = "performancemesure";

    private static final String CSTJSON_PERFORMANCEMESURETOTAL = "total";

    ProcessAPI processAPI;
    long tenantId;

    public PurgeTablesMessage(ProcessAPI processAPI, long tenantId) {
        this.processAPI = processAPI;
        this.tenantId = tenantId;
    }
    
    private static class Query {

        protected String sqlQueryCount;
        protected String sqlQueryPurge;
        protected String label;
        protected String explanation;

        protected Query(String queryCount, String queryPurge, String label, String explanation) {
            this.sqlQueryCount = queryCount;
            this.sqlQueryPurge = queryPurge;
            this.label = label;
            this.explanation = explanation;
        }
    }

    private final Query[] listQueries = { new Query(MessagesFactory.SQLQUERY_COUNT_MESSAGESNOMOREPROCESS, MessagesFactory.SQLUPDATE_PURGE_MESSAGESNOMOREPROCESS, "Messages without process", "Message in database reference a target process which not exists (table message_instance reference a targetprocessname unknown)"),
            new Query(MessagesFactory.SQLQUERY_COUNT_WAITINGEVENTNOMOREPROCESS, MessagesFactory.SQLUPDATE_PURGE_WAITINGEVENTNOMOREPROCESS, "Event without Process", "Event in database reference a process which at not exists (table waiting_event reference an unknow processDefinition)"),
            new Query(MessagesFactory.SQLQUERY_COUNT_WAITINGEVENTNOMOREPROCESSINSTANCE, MessagesFactory.SQLUPDATE_PURGE_WAITINGEVENTNOMOREPROCESSINSTANCE, "Archived process instance", "Event in database reference an archived process instance (table waiting_event reference a archived process instance)"),
            new Query(MessagesFactory.SQLQUERY_COUNT_DATAWITHOUTMESSAGE, MessagesFactory.SQLUPDATE_PURGE_DATAWITHOUTMESSAGE, "Message data without message", "Some data are referenced into a message (table data_instance), but the message does not exists anymore (in table message_instance)")
    };

    /**
     * Result classes
     */
    public class ResultOnePurge {

        protected Query query;
        protected long nbRecords;
        protected long nbRecordsUpdated;
        protected boolean purgeIsOk;

        public Map<String, Object> getMap() {
            Map<String, Object> result = new HashMap<>();
            result.put("nbrecords", nbRecords);
            result.put("nbrecordspurged", nbRecordsUpdated);            
            result.put("purgeisok", purgeIsOk);
            result.put("label", query.label);
            result.put("explanation", query.explanation);
            result.put("querydeletion", query.sqlQueryPurge);
            
            return result;
        }
    }

    public class ResultPurge {

        protected List<ResultOnePurge> listResultPurges = new ArrayList<>();
        protected List<BEvent> listEvents = new ArrayList<>();
        protected PerformanceMesureSet performanceMesure = new PerformanceMesureSet();

        public Map<String, Object> getMap() {
            Map<String, Object> result = new HashMap<>();
            List<Map<String, Object>> listResultsMap = new ArrayList<>();
            result.put("listitems", listResultsMap);
            for (ResultOnePurge resultOnePurge : listResultPurges) {
                listResultsMap.add(resultOnePurge.getMap());
            }
            result.put("listevents", BEventFactory.getHtml(listEvents));
            result.put( CSTJSON_PERFORMANCEMESURE, performanceMesure.getMap());

            return result;
        }
    }

    /**
     * get the list of different request
     * 
     * @param processAPI
     * @return
     */
    public ResultPurge getListMonitoringItems() {
        ResultPurge resultPurge = new ResultPurge();
        PerformanceMesure perf = resultPurge.performanceMesure.getMesure(CSTJSON_PERFORMANCEMESURETOTAL);
        perf.start();
     
        MessagesFactory messagesFactory = new MessagesFactory( tenantId );
        try (Connection con = MessagesFactory.getConnection();) {

            for (Query query : listQueries) {
                ResultOnePurge resultOnePurge = new ResultOnePurge();
                ResultQuery resultQuery = messagesFactory.executeOneResultQuery("monitorquery", query.sqlQueryCount, null, 0, con);
                resultPurge.performanceMesure.add(resultQuery.getPerformanceMesure() );
                resultOnePurge.nbRecords = messagesFactory.getLong(resultQuery.getOneResult(), 0L);
                resultOnePurge.query = query;
                resultPurge.listResultPurges.add(resultOnePurge);
                resultPurge.listEvents.addAll( resultQuery.getListEvents());
            }

        } catch (Exception e) {
            resultPurge.listEvents.add(new BEvent(eventSqlQueryError, e, "Message:" + e.getMessage()));
        }
        perf.stop();
        return resultPurge;

    }

    /**
     * @param processAPI
     * @return
     */
    public ResultPurge updateListMonitoringItems() {
        ResultPurge resultPurge = new ResultPurge();
        PerformanceMesure perf = resultPurge.performanceMesure.getMesure(CSTJSON_PERFORMANCEMESURETOTAL);
        perf.start();
     
        MessagesFactory messagesFactory = new MessagesFactory(tenantId);
        try (Connection con = MessagesFactory.getConnection();) {

            for (Query query : listQueries) {
                ResultOnePurge resultOnePurge = new ResultOnePurge();
                ResultQuery resultQuery = messagesFactory.executeUpdateQuery("purgeItem", query.sqlQueryPurge, null, con, true);
                resultPurge.performanceMesure.add(resultQuery.getPerformanceMesure() );
                resultPurge.listEvents.addAll( resultQuery.getListEvents());
                resultOnePurge.nbRecordsUpdated = resultQuery.getNumberOfRows();
                resultPurge.listEvents.addAll( resultQuery.getListEvents());
                
                // reexecute the query to have a status
                resultQuery = messagesFactory.executeOneResultQuery("countItem", query.sqlQueryCount, null, 0, con);
                resultPurge.performanceMesure.add(resultQuery.getPerformanceMesure() );
                resultOnePurge.nbRecords = messagesFactory.getLong(resultQuery.getOneResult(), 0L);

                resultPurge.listEvents.addAll( resultQuery.getListEvents());
                resultOnePurge.query = query;
                resultOnePurge.purgeIsOk = true;

                resultPurge.listResultPurges.add(resultOnePurge);
            }

        } catch (Exception e) {
            resultPurge.listEvents.add(new BEvent(eventSqlQueryError, e, "Message:" + e.getMessage()));
        }
        perf.stop();
        return resultPurge;

    }

}
