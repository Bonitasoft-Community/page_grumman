package org.bonitasoft.grumman;

import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

import org.bonitasoft.engine.api.ProcessAPI;
import org.bonitasoft.grumman.message.MessagesFactory;
import org.bonitasoft.grumman.purge.DuplicateMessageInstance;
import org.bonitasoft.grumman.purge.PurgeTablesMessage;
import org.bonitasoft.grumman.reconciliation.ReconcilationMessage;
import org.bonitasoft.grumman.reconciliation.ReconcilationMessage.ReconcialiationFilter;
import org.bonitasoft.grumman.reconciliation.ReconcilationMessage.ReconcialiationFilter.TYPEFILTER;
import org.json.simple.JSONValue;

public class GrummanAPI {

    private final static Logger logger = Logger.getLogger(MessagesFactory.class.getName());

    private static String loggerLabel = "GrummanAPI ##";


    ProcessAPI processAPI;
    MessagesFactory messageFactory;

    public GrummanAPI(ProcessAPI processAPI) {
        this.processAPI = processAPI;
        this.messageFactory = new MessagesFactory();
    }

    public Map<String, Object> getSynthesis() {
        return messageFactory.getSynthesis().getMap();
    }

    /* -------------------------------------------------------------------- */
    /*                                                                      */
    /* Reconciliation */
    /*                                                                      */
    /* -------------------------------------------------------------------- */

    public Map<String, Object> getIncompleteReconciliationMessage(int numberOfMessages, ProcessAPI processAPI) {
        ReconcilationMessage reconciliation = new ReconcilationMessage();
        ReconcialiationFilter reconciliationFilter = new ReconcialiationFilter( TYPEFILTER.MAXMESSAGES);
        reconciliationFilter.numberOfMessages = (numberOfMessages<10 ? 10 : numberOfMessages);
        return reconciliation.getListIncompleteMessage(reconciliationFilter, processAPI).getGroupByMap();

    }

    public static class MessagesList {
        
        private List<String> listKeys = null;
        private List<String> listKeysGroups = null;
        private int numberofmessages=-1;
        /** 
         * It may have multiple (BAD) message_event with the same key. Let's say there are for 2 waiting event, 10 messages_event. 
         * So, 2 messages is sent to unlock the 2 waiting_event. Do we purge only 2 message_event or ALL message events? 
         */
        
        private boolean purgeAllRelativesId = false;
        
        @SuppressWarnings({ "unchecked", "rawtypes" })        
        public static MessagesList getInstanceFromJson(String jsonSt ) {
            logger.info( loggerLabel+" decodeFromJsonSt : JsonSt[" + jsonSt + "]");
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
            messagesList.numberofmessages   =  MessagesFactory.getLong( mapObject.get("numberofmessages"),-1L).intValue();
            messagesList.purgeAllRelativesId=  MessagesFactory.getBoolean( mapObject.get("purgeallrelatives"), false);
            return messagesList;
        }
        public int getNumberOfMessages() {
            return numberofmessages;
        }
        public boolean getPurgeAllRelatives() {
            return purgeAllRelativesId;
        }
        public List<String> getListKeys() {
            return listKeys;
        }
        public List<String> getListKeysGroup() {
            return listKeysGroups;
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
        ReconcilationMessage reconciliation = new ReconcilationMessage();
        return reconciliation.executeIncompleteMessage( messagesList, processAPI).getMap();

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
