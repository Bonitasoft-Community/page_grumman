package org.bonitasoft.grumman.purge;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

import org.bonitasoft.engine.api.ProcessAPI;
import org.bonitasoft.grumman.GrummanAPI.MessagesIdList;
import org.bonitasoft.grumman.GrummanAPI.MessagesList;
import org.bonitasoft.grumman.message.Message;
import org.bonitasoft.grumman.message.MessagesFactory;
import org.bonitasoft.grumman.message.MessagesFactory.ResultQuery;
import org.bonitasoft.grumman.performance.PerformanceMesureSet;
import org.bonitasoft.grumman.performance.PerformanceMesureSet.PerformanceMesure;
import org.bonitasoft.grumman.purge.PurgeTablesMessage.ResultOnePurge;
import org.bonitasoft.log.event.BEvent;
import org.bonitasoft.log.event.BEvent.Level;
import org.json.simple.JSONValue;
import org.bonitasoft.log.event.BEventFactory;

public class DuplicateMessageInstance {
    
    private final static Logger logger = Logger.getLogger(MessagesFactory.class.getName());

    private static String loggerLabel = "DuplicateMessageInstance ##";

    private static final String CSTJSON_LISTEVENTS= "listevents";

    private static final String CSTJSON_PERFORMANCEMESURE = "performancemesure";
    private static final String CSTJSON_PERFORMANCEMESURETOTAL = "total";

    private final static BEvent eventSqlQueryError = new BEvent(MessagesFactory.class.getName(), 1, Level.ERROR, "Purge query error", "A Purge Sql query has an error", "No result for the query", "check exception");

    private String[] idToExport = {"id", "messagename", "targetprocess", "targetflownode" };
    
    /**
     * Result classes
     */
    public class ResultDuplicateMessage {

        protected Map<String,Object> messageinstance;
        
        protected List<Long> listIdDuplicate = new ArrayList<>();

        public Long getMessageId() {
            return MessagesFactory.getLong( messageinstance.get("id"),0L );
        }
        
        public Map<String, Object> getMap() {
            Map<String, Object> result = new HashMap<>();
            for (String id : idToExport)
                result.put( id, messageinstance.get( id ));
            result.put("nbduplications", listIdDuplicate.size()+1);
            Object correlationValues[] = new Object[ 5 ];
            for (int i=0;i<5;i++) {
                correlationValues[ i ]= messageinstance.get(Message.listColumnCorrelation[ i ]);
            }
            result.put("correlationvalues", Message.getCorrelationSignature(correlationValues));
            List<Map<String,Object>> listDetails = new ArrayList<>();
            
            for (Long id : listIdDuplicate) 
            {
                Map<String, Object> detail = new HashMap<>();
                detail.put("wid", id);
                listDetails.add( detail );
            }
            
            
            result.put("messagesduplicated", listDetails);
            return result;
        }
    }
    public class ResultDuplicate {
        protected PerformanceMesureSet performanceMesure = new PerformanceMesureSet();

        protected long nbMessagesDuplicated;
        protected List<ResultDuplicateMessage> listDuplications = new ArrayList<>();
        protected List<BEvent> listEvents;

        public Map<String, Object> getMap() {
            Map<String, Object> result = new HashMap<>();
            List<Map<String,Object>> listDuplicateMessageMap = new ArrayList<>();
            for (ResultDuplicateMessage duplicateMessage : listDuplications)
            {
                listDuplicateMessageMap.add( duplicateMessage.getMap());
            }
            result.put( CSTJSON_LISTEVENTS, BEventFactory.getHtml(listEvents));
            result.put( "listduplications", listDuplicateMessageMap);
            result.put( "nbMessagesDuplicated", nbMessagesDuplicated);            
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
    public ResultDuplicate getListDuplicateMessages(int maximumNumberOfDuplications, ProcessAPI processAPI) {
        ResultDuplicate resultDuplicate = new ResultDuplicate();
        MessagesFactory messagesFactory = new MessagesFactory();
        PerformanceMesure perf = resultDuplicate.performanceMesure.getMesure(CSTJSON_PERFORMANCEMESURETOTAL);
        perf.start();

        try (Connection con = MessagesFactory.getConnection();) {

            // lets assume there are maximum 10 duplications per message
            ResultQuery resultQuery = messagesFactory.executeListResultQuery("duplicate", MessagesFactory.SQLQUERY_SEARCHDUPLICATEMESSAGEINSTANCE, null, maximumNumberOfDuplications*10, null, con);
            resultDuplicate.listEvents = resultQuery.getListEvents();
            resultDuplicate.performanceMesure.add( resultQuery.performanceMesure);
            
            ResultDuplicateMessage currentMessage = null;
            for (Map<String,Object> map : resultQuery.getListResult()) {
                long currentId = MessagesFactory.getLong( map.get("id"), -1L);
                
                if (currentMessage == null || (currentId != currentMessage.getMessageId())) {
                    // a new one
                    if (resultDuplicate.listDuplications.size()>= maximumNumberOfDuplications)
                        break;
                    currentMessage = new ResultDuplicateMessage();
                    resultDuplicate.listDuplications.add( currentMessage );
                    currentMessage.messageinstance = map;
                    currentMessage.listIdDuplicate.add( MessagesFactory.getLong( map.get("id"), 0L) );

                }
                else
                {
                    currentMessage.listIdDuplicate.add( MessagesFactory.getLong( map.get("duplicate_id"), 0L) );
                }
            }

        } catch (Exception e) {
            resultDuplicate.listEvents.add(new BEvent(eventSqlQueryError, e, "Message:" + e.getMessage()));
        }
        perf.stop();

        return resultDuplicate;

    }

    /**
     * Purge
     * @author Firstname Lastname
     *
     */
    public class ResultPurge {

        protected int nbRecordsDeleted =0;;
        protected List<BEvent> listEvents = new ArrayList<>();
        protected PerformanceMesureSet performanceMesure = new PerformanceMesureSet();

        public Map<String, Object> getMap() {
            Map<String, Object> result = new HashMap<>();
            
            result.put("nbrecordsdeleted", nbRecordsDeleted);
            result.put("listevents", BEventFactory.getHtml(listEvents));
            result.put( CSTJSON_PERFORMANCEMESURE, performanceMesure.getMap());

            return result;
        }
    }
    
 
    /**
     * @param processAPI
     * @return
     */
    public ResultPurge deleteDuplicateMessages(MessagesIdList listIdToPurge, ProcessAPI processAPI) {
        ResultPurge resultPurge = new ResultPurge();
        PerformanceMesure perf = resultPurge.performanceMesure.getMesure(CSTJSON_PERFORMANCEMESURETOTAL);
        perf.start();

        MessagesFactory messagesFactory = new MessagesFactory();
        try (Connection con = MessagesFactory.getConnection();) {

            for (Long id : listIdToPurge.listIds) {
                List<Object> parameters = new ArrayList<>();
                parameters.add(id);
                ResultQuery resultQuery = messagesFactory.executeUpdateQuery("deleteduplicate", MessagesFactory.SQLQUERY_PURGEDUPLICATEMESSAGEINSTANCE, parameters, con, true);
                resultPurge.listEvents.addAll( resultQuery.getListEvents());
                resultPurge.nbRecordsDeleted = resultQuery.getNumberOfRows();
                resultPurge.performanceMesure.add( resultQuery.performanceMesure);

            }

        } catch (Exception e) {
            resultPurge.listEvents.add(new BEvent(eventSqlQueryError, e, "Message:" + e.getMessage()));
        }
        perf.stop();

        return resultPurge;

    }

}
