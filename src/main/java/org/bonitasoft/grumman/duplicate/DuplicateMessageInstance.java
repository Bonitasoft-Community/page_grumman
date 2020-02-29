package org.bonitasoft.grumman.duplicate;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

import org.bonitasoft.engine.api.ProcessAPI;
import org.bonitasoft.grumman.GrummanAPI;
import org.bonitasoft.grumman.GrummanAPI.MessagesIdList;
import org.bonitasoft.grumman.message.Message;
import org.bonitasoft.grumman.message.MessagesFactory;
import org.bonitasoft.grumman.message.MessagesFactory.ResultPurge;
import org.bonitasoft.grumman.message.MessagesFactory.ResultQuery;
import org.bonitasoft.grumman.performance.PerformanceMesureSet;
import org.bonitasoft.grumman.performance.PerformanceMesureSet.PerformanceMesure;
import org.bonitasoft.log.event.BEvent;
import org.bonitasoft.log.event.BEvent.Level;
import org.bonitasoft.log.event.BEventFactory;

import lombok.Data;

public class DuplicateMessageInstance {
    
    private final static Logger logger = Logger.getLogger(MessagesFactory.class.getName());

    private static String loggerLabel = "DuplicateMessageInstance ##";

  
    private final static BEvent eventSqlQueryError = new BEvent(MessagesFactory.class.getName(), 1, Level.ERROR, "Purge query error", "A Purge Sql query has an error", "No result for the query", "check exception");

    private String[] idToExport = {"id", "messagename", "targetprocess", "targetflownode" };
    
    /**
     * Result classes
     */
    public @Data class ResultDuplicateMessage {

        protected Map<String,Object> messageinstance;
        protected Long originalId;

        protected List<Long> listIdDuplicate = new ArrayList<>();

        public Long getMessageId() {
            return MessagesFactory.getLong( messageinstance.get("id"),0L );
        }
        
        public Map<String, Object> getMap() {
            Map<String, Object> result = new HashMap<>();
            for (String id : idToExport)
                result.put( id, messageinstance.get( id ));
            result.put("nbduplications", listIdDuplicate.size());
            result.put( GrummanAPI.CSTJSON_ORIGINALID, originalId);  

            Object correlationValues[] = new Object[ 5 ];
            for (int i=0;i<5;i++) {
                correlationValues[ i ]= messageinstance.get(Message.listColumnCorrelation[ i ]);
            }
            result.put( GrummanAPI.CSTJSON_CORRELATIONVALUES, Message.getCorrelationSignature(correlationValues, Message.CST_DEFAULTJSON_KEEPNONE_CORRELATIONSIGNATURE));

            List<Map<String,Object>> listDetails = new ArrayList<>();
            
            for (Long id : listIdDuplicate) 
            {
                Map<String, Object> detail = new HashMap<>();
                detail.put("mid", id);
                listDetails.add( detail );
            }
            
            
            result.put("messagesduplicated", listDetails);
            return result;
        }
    }
    public @Data class ResultDuplicate {
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
            result.put( GrummanAPI.CSTJSON_LISTEVENTS, BEventFactory.getHtml(listEvents));
            result.put( GrummanAPI.CSTJSON_LISTDUPLICATION, listDuplicateMessageMap);
            result.put( GrummanAPI.CSTJSON_NBMESSAGEDUPLICATED, nbMessagesDuplicated);  
            
            result.put( GrummanAPI.CSTJSON_PERFORMANCEMESURE, performanceMesure.getMap());

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
        PerformanceMesure perf = resultDuplicate.performanceMesure.getMesure( GrummanAPI.CSTJSON_PERFORMANCEMESURETOTAL);
        perf.start();
        try (Connection con = MessagesFactory.getConnection();) {

            // lets assume there are maximum 10 duplications per message
            ResultQuery resultQuery = messagesFactory.executeListResultQuery("duplicate", MessagesFactory.SQLQUERY_SEARCHDUPLICATEMESSAGEINSTANCE, null, maximumNumberOfDuplications*10, null, con);
            resultDuplicate.listEvents = resultQuery.getListEvents();
            resultDuplicate.performanceMesure.add( resultQuery.getPerformanceMesure());
            
            ResultDuplicateMessage currentMessage = null;
            for (Map<String,Object> map : resultQuery.getListResult()) {
                long currentId = MessagesFactory.getLong( map.get("id"), -1L);
                
                if (currentMessage == null || (currentId != currentMessage.getMessageId())) {
                    // a new one
                    if (resultDuplicate.listDuplications.size()>= maximumNumberOfDuplications)
                        break;
                    currentMessage = new ResultDuplicateMessage();
                    currentMessage.setOriginalId(MessagesFactory.getLong( map.get("id"), 0L) );
                    resultDuplicate.listDuplications.add( currentMessage );
                    currentMessage.messageinstance = map;
                    currentMessage.listIdDuplicate.add( MessagesFactory.getLong( map.get("duplicate_id"), 0L) );

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
        logger.info(loggerLabel+"-- getListDuplicateMessages, detect "+resultDuplicate.nbMessagesDuplicated+" msg duplicate");

        return resultDuplicate;

    }

    /**
     * Purge
     * @author Firstname Lastname
     *
     */
    
    public @Data class ResultPurgeDuplicate {
        private int nbDatasRowDeleted=0;
        private int nbMessagesRowDeleted=0;
        private List<Long> listIdMessageInstancePurged;
        private List<BEvent> listEvents = new ArrayList<>();
        private PerformanceMesureSet performanceMesure = new PerformanceMesureSet();

        public Map<String, Object> getMap() {
            Map<String, Object> result = new HashMap<>();
            
            result.put( GrummanAPI.CSTJSON_LISTEVENTS, BEventFactory.getHtml(listEvents));
            result.put( GrummanAPI.CSTJSON_PERFORMANCEMESURE, performanceMesure.getMap());
            result.put( GrummanAPI.CSTJSON_NB_DATASROW_DELETED,  nbDatasRowDeleted);
            result.put( GrummanAPI.CSTJSON_ND_MESSAGESROW_DELETED,  nbMessagesRowDeleted);
            result.put( GrummanAPI.CSTJSON_LISTMESSAGEINSTANCEPURGED, listIdMessageInstancePurged);
            return result;
        }
    }
    
 
    /**
     * @param processAPI
     * @return
     */
    public ResultPurgeDuplicate deleteDuplicateMessages(MessagesIdList listIdToPurge, ProcessAPI processAPI) {
        ResultPurgeDuplicate resultPurgeDuplicate = new ResultPurgeDuplicate();
        PerformanceMesure perf = resultPurgeDuplicate.performanceMesure.getMesure( GrummanAPI.CSTJSON_PERFORMANCEMESURETOTAL);
        perf.start();

        MessagesFactory messagesFactory = new MessagesFactory();
        try (Connection con = MessagesFactory.getConnection();) {

            ResultPurge resultPurge =  messagesFactory.purgeMessageInstance( listIdToPurge.listIds, true, 0);
    
            resultPurgeDuplicate.listEvents.addAll( resultPurge.getListEvents());
            resultPurgeDuplicate.performanceMesure.add( resultPurge.getPerformanceMesure());
            resultPurgeDuplicate.nbDatasRowDeleted = resultPurge.getNbDatasRowDeleted();
            resultPurgeDuplicate.nbMessagesRowDeleted= resultPurge.getNbMessagesRowDeleted();
            resultPurgeDuplicate.listIdMessageInstancePurged = resultPurge.getListIdMessageInstancePurged();

            

        } catch (Exception e) {
            resultPurgeDuplicate.listEvents.add(new BEvent(eventSqlQueryError, e, "Message:" + e.getMessage()));
        }
        perf.stop();

        return resultPurgeDuplicate;

    }

}
