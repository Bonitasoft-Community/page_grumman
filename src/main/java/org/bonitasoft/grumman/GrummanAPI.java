package org.bonitasoft.grumman;


import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

import org.bonitasoft.engine.api.ProcessAPI;
import org.bonitasoft.grumman.duplicate.DuplicateMessageInstance;
import org.bonitasoft.grumman.message.Message;
import org.bonitasoft.grumman.message.MessagesFactory;
import org.bonitasoft.grumman.message.MessagesFactory.SynthesisOnMessage;
import org.bonitasoft.grumman.purge.PurgeTablesMessage;
import org.bonitasoft.grumman.reconciliation.ReconcilationMessage;
import org.bonitasoft.grumman.reconciliation.ReconcilationMessage.ReconcialiationFilter;
import org.bonitasoft.grumman.reconciliation.ReconcilationMessage.ReconcialiationFilter.TYPEFILTER;
import org.bonitasoft.grumman.reconciliation.ReconcilationMessage.ResultExecution;
import org.bonitasoft.grumman.reconciliation.ReconcilationMessage.ResultMessageOperation;

import org.json.simple.JSONValue;

import lombok.Data;

public class GrummanAPI {

    private final static Logger logger = Logger.getLogger(GrummanAPI.class.getName());

    private static String loggerLabel = "GrummanAPI ##";

    /**
     * All JSON constant exchanded with the page
     */
    public static final String CSTJSON_LISTMESSAGES = "listmessages";
    public static final String CSTJSON_NBCOMPLETEMESSAGES="nbCompleteMessages";
    public static final String CSTJSON_NBINCOMPLETEMESSAGE ="nbIncompleteMessages";

    public static final String CSTJSON_NBMESSAGE = "nbmessage";
    public static final String CSTJSON_LISTEVENTS= "listevents";
    public static final String CSTJSON_NBRECONCILIATIONS= "nbReconciliations";
    public static final String CSTJSON_WID = "wid";
    public static final String CSTJSON_MID = "mid";
    public static final String CSTJSON_CASEID =     "caseid";
    public static final String CSTJSON_STATUS = "status";
    public static final String CSTJSON_NBEXECUTIONINPROGRESS = "nbexecutioninprogress";
    public static final String CSTJSON_LISTMESSAGEINSTANCERELATIVE     = "listmessageinstancerelative";
    public static final String CSTJSON_LISTMESSAGEINSTANCERELATIVEPURGED= "listmessageinstancerelativepurged";
    public static final String CSTJSON_LISTMESSAGEINSTANCEPURGED= "listmessageinstancepurged";
    public static final String CSTJSON_STATUSEXEC = "statusexec";

    public static final String CSTJSON_PROCESSNAME="processname";
    public static final String CSTJSON_PROCESSVERSION="processversion";
    public static final String CSTJSON_FLOWNAME="flowname";
    
    public static final String CSTJSON_NUMBEROFMESSAGES="numberofmessages";
    public static final String CSTJSON_KEYGROUP="keygroup";

    
    public static final String CSTJSON_MESSAGENAME = "messagename";
    public static final String CSTJSON_CATCHEVENTTYPE ="catcheventtype";
    public static final String CSTJSON_EXPL = "expl";
    public static final String CSTJSON_EXPLEXEC = "explexec";
    public static final String CSTJSON_EXPLERROR= "explerror";
    public static final String CSTJSON_CORRELATIONVALUES = "correlationvalues";
    public static final String CSTJSON_SIGNATURENBMESSAGEINSTANCE = "signaturenbmessageinstance";
    public static final String CSTJSON_SIGNATURENBWAITINGEVENT ="signaturenbwaitingevent";
    
    public static final String CSTJSON_PERFORMANCEMESURE = "performancemesure";
    public static final String CSTJSON_PERFORMANCEMESURETOTAL = "total";

    public static final String CSTJSON_DETAILS = "details";
    
    public static final String CSTJSON_NB_DATASROW_DELETED = "nbdatasrowdeleted";
    public static final String CSTJSON_ND_MESSAGESROW_DELETED ="nbmessagesrowdeleted";
    public static final String CSTJSON_MESSAGES_ERRORS="messageserrors";
    public static final String CSTJSON_MESSAGES_CORRECTS="messagescorrects";
 
    
    public static final String CSTJSON_LISTDUPLICATION = "listduplications";
    public static final String CSTJSON_NBMESSAGEDUPLICATED ="nbMessagesDuplicated";            
    public static final String CSTJSON_ORIGINALID = "originalid";
    
    
    ProcessAPI processAPI;
    MessagesFactory messageFactory;

    public GrummanAPI(ProcessAPI processAPI) {
        this.processAPI = processAPI;
        this.messageFactory = new MessagesFactory();
    }

    public Map<String, Object> getSynthesis() {
        return messageFactory.getSynthesis().getMap();
    }
    public SynthesisOnMessage getSynthesisMessage() {
        return messageFactory.getSynthesis();
    }

    /* -------------------------------------------------------------------- */
    /*                                                                      */
    /* Reconciliation */
    /*                                                                      */
    /* -------------------------------------------------------------------- */

    public Map<String, Object> getIncompleteReconciliationMessage(int numberOfMessages, ProcessAPI processAPI) {
        ReconcialiationFilter reconciliationFilter = new ReconcialiationFilter( TYPEFILTER.MAXMESSAGES);
        reconciliationFilter.numberOfMessages = (numberOfMessages<10 ? 10 : numberOfMessages);
        
        ResultMessageOperation resultMessageOperation =  getIncompleteReconciliationMessage( reconciliationFilter, processAPI);
        return resultMessageOperation.getGroupByMap();
        

    }
    
    public ResultMessageOperation getIncompleteReconciliationMessage(ReconcialiationFilter reconciliationFilter, ProcessAPI processAPI) {
        ReconcilationMessage reconciliation = new ReconcilationMessage();
        return reconciliation.getListIncompleteMessage(reconciliationFilter, processAPI);
        
    }
    

    public static @Data class MessagesList {
        
        private List<String> listKeys = null;
        private List<String> listKeysGroups = null;
        private int numberOfMessages=-1;
        /** 
         * It may have multiple (BAD) message_event with the same key. Let's say there are for 2 waiting event, 10 messages_event. 
         * So, 2 messages is sent to unlock the 2 waiting_event. Do we purge only 2 message_event or ALL message events? 
         */
        
        private boolean purgeAllRelativesId = false;
        private boolean sendincomplete = false;
        private boolean executecomplete = false;
        
      
        public static MessagesList getInstanceFromResultMessageOperation(ResultMessageOperation resultMessageOperation ) {
            MessagesList messagesList = new MessagesList();
            Map<String, List<Message>> mapMessages = resultMessageOperation.getMessageByGroup();
            messagesList.listKeysGroups = new ArrayList<>();
            messagesList.listKeysGroups.addAll( mapMessages.keySet());
            return messagesList;
          }
            
        @SuppressWarnings({ "unchecked", "rawtypes" })        
        public static MessagesList getInstanceFromJson(String jsonSt ) {
            logger.fine( loggerLabel+" decodeFromJsonSt : JsonSt[" + jsonSt + "]");
            MessagesList messagesList = new MessagesList();

            if (jsonSt == null) {
                return messagesList;
            }

                // we can get 2 type of JSON :
                // { 'processes' : [ {..}, {...} ], 'scenarii':[{...}, {...},
                // 'process' : {..}, 'scenario': {} ] }
                // or a list of order
                // [ { 'process': {}, 'process': {}, 'scenario': {..}];

            final Object jsonObject = JSONValue.parse(jsonSt);
            if (jsonObject == null)
                return messagesList;
            Map<String,Object> mapObject = (Map) jsonObject;
            
            messagesList.listKeys = mapObject.containsKey("keys")? (List) mapObject.get("keys"):null;
            messagesList.listKeysGroups = mapObject.containsKey("keysgroup")? (List) mapObject.get("keysgroup"):null;
            messagesList.numberOfMessages   =  MessagesFactory.getLong( mapObject.get("numberofmessages"),-1L).intValue();
            messagesList.purgeAllRelativesId =  MessagesFactory.getBoolean( mapObject.get("purgeallrelatives"), false);
            messagesList.sendincomplete =  MessagesFactory.getBoolean( mapObject.get("sendincomplete"), false);
            messagesList.executecomplete =  MessagesFactory.getBoolean( mapObject.get("executecomplete"), false);

            return messagesList;
        }
       
        public boolean isBorrowKeys() {
            return listKeys != null;
        }

    }
    /**
     * A message is send, but can't be executed because all the variables are not fullfilled.
     * So,
     * 1/ all messages reconcialed are checked
     * 2/ if a message does not contains all mandatory field, a new message is created, with all mandatory field
     * 3/ all messages waiting with not all the mandatory fields are purged
     * 
     * @param processAPI
     * @return
     */
    public Map<String, Object> sendIncompleteReconcialiationMessage( MessagesList messagesList, ProcessAPI processAPI) {
        return executeReconcialiationMessage( messagesList, processAPI).getMap();
    }

    public ResultExecution executeReconcialiationMessage( MessagesList messagesList, ProcessAPI processAPI) {
        ReconcilationMessage reconciliation = new ReconcilationMessage();
        return reconciliation.executeIncompleteMessage( messagesList, processAPI);
    }

    
    /**
     * 
     * @param processAPI
     * @return
     */
    public Map<String, Object> getPurgeTablesMonitoring( ProcessAPI processAPI) {
        PurgeTablesMessage purgeTable = new PurgeTablesMessage();
        return purgeTable.getListMonitoringItems(processAPI).getMap();
    }
    
    public Map<String, Object> purgeTablesMonitoring( ProcessAPI processAPI) {
        PurgeTablesMessage purgeTable = new PurgeTablesMessage();
        return purgeTable.updateListMonitoringItems(processAPI).getMap();

    }

    
   public static class MessagesIdList {
        
        public List<Long> listIds = null;
        
        
        @SuppressWarnings({ "unchecked", "rawtypes" })        
        public static MessagesIdList getInstanceFromJson(String jsonSt ) {
            logger.info( loggerLabel+" decodeFromJsonSt : JsonSt[" + jsonSt + "]");
            MessagesIdList listIds = new MessagesIdList();

            if (jsonSt == null) {
                return listIds;
            }

                // we can get 2 type of JSON :
                // { 'processes' : [ {..}, {...} ], 'scenarii':[{...}, {...},
                // 'process' : {..}, 'scenario': {} ] }
                // or a list of order
                // [ { 'process': {}, 'process': {}, 'scenario': {..}];

            final Object jsonObject = JSONValue.parse(jsonSt);
            if (jsonObject == null)
                return listIds;
            Map<String,Object> mapObject = (Map) jsonObject;
            
            listIds.listIds = mapObject.containsKey("keys")? (List) mapObject.get("keys"):null;
            return listIds;
        }
   }
    /**
     * Search duplicate message
     * @param processAPI
     * @return
     */
    public Map<String, Object> getListDuplicateMessages( int maximumNmberOfDuplications, ProcessAPI processAPI) {
        DuplicateMessageInstance duplicateMessages = new DuplicateMessageInstance();
        return duplicateMessages.getListDuplicateMessages(maximumNmberOfDuplications, processAPI).getMap();
    }
    
    public Map<String, Object> deleteDuplicateMessages(MessagesIdList listIds,  ProcessAPI processAPI) {
        DuplicateMessageInstance duplicateMessages = new DuplicateMessageInstance();
        return duplicateMessages.deleteDuplicateMessages(listIds,processAPI).getMap();

    }

    // status :
    // nombre de waiting event, de message instance
    // nombre de reconciliation
    // waiting_event le plus ancien, message_instance le plus ancien
    // repartition par processus
    // message.handle: en cours de traitement (flague pour un work)
    // waiting_event.progress => Same 

}
