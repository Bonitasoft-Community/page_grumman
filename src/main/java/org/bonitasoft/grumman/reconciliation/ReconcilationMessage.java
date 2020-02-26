package org.bonitasoft.grumman.reconciliation;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.bonitasoft.engine.api.ProcessAPI;
import org.bonitasoft.engine.bpm.process.DesignProcessDefinition;
import org.bonitasoft.grumman.GrummanAPI.MessagesList;
import org.bonitasoft.grumman.message.Message;
import org.bonitasoft.grumman.message.Message.enumStatus;
import org.bonitasoft.grumman.message.MessagesFactory;
import org.bonitasoft.grumman.message.MessagesFactory.ResultOperation;
import org.bonitasoft.grumman.message.MessagesFactory.ResultPurge;
import org.bonitasoft.grumman.performance.PerformanceMesureSet;
import org.bonitasoft.grumman.performance.PerformanceMesureSet.PerformanceMesure;
import org.bonitasoft.grumman.reconciliation.ReconcilationMessage.ReconcialiationFilter.TYPEFILTER;
import org.bonitasoft.log.event.BEvent;
import org.bonitasoft.log.event.BEvent.Level;
import org.bonitasoft.log.event.BEventFactory;

public class ReconcilationMessage {

    private final static BEvent eventSqlNoMessagesSelected = new BEvent(ReconcilationMessage.class.getName(), 1, Level.APPLICATIONERROR, "No messages selected", "Select minimum one message", "", "Select one message");

    
    private static final String CSTJSON_LISTEVENTS= "listevents";
    private static final String CSTJSON_NBRECONCILIATIONS= "nbReconciliations";
    private static final String CSTJSON_WID = "wid";
    private static final String CSTJSON_MID = "mid";
    private static final String CSTJSON_CASEID =     "caseid";
    private static final String CSTJSON_STATUS = "status";
    private static final String CSTJSON_NBEXECUTIONINPROGRESS = "nbexecutioninprogress";
    private static final String CSTJSON_STATUSEXEC = "statusexec";
    private static final String CSTJSON_MESSAGENAME = "messagename";
    private static final String CSTJSON_CATCHEVENTTYPE ="catcheventtype";
    private static final String CSTJSON_EXPL = "expl";
    private static final String CSTJSON_CORRELATIONVALUES = "correlationvalues";
    private static final String CSTJSON_PERFORMANCEMESURE = "performancemesure";
    private static final String CSTJSON_PERFORMANCEMESURETOTAL = "total";
    
    public static class ResultMessageOperation {

        protected List<BEvent> listEvents = new ArrayList<>();
        protected List<Message> listMessages = new ArrayList<>();
        protected int nbReconciliations = 0;
        protected int nbCompleteMessages = 0;
        protected int nbIncompleteMessages = 0;

        
        protected PerformanceMesureSet performanceMesure = new PerformanceMesureSet();
 
        public Map<String, Object> getMap() {
            Map<String, Object> result = new HashMap<>();
            List<Map<String, Object>> listMessageMap = new ArrayList<>();
            for (Message message : listMessages)
                listMessageMap.add(message.getMap());

            result.put("listmessages", listMessageMap);
            result.put("nbmessage", listMessages.size());
            result.put( CSTJSON_LISTEVENTS, BEventFactory.getSyntheticHtml(listEvents));
            result.put( CSTJSON_NBRECONCILIATIONS, nbReconciliations);
            result.put("nbCompleteMessages", nbCompleteMessages);
            result.put("nbIncompleteMessages", nbIncompleteMessages);

            return result;
        }

        public Map<String, Object> getGroupByMap() {
            // result are grouped per processname/processversion/flownodename
            Map<String, Object> result = new HashMap<>();
            Map<String, List<Message>> mapMessages = new HashMap<>();
            for (Message message : listMessages) {
                String key = message.targetProcessName + "#" + message.currentProcessVersion + "#" + message.targetFlowNodeName + "#" + message.status.toString();
                List<Message> list = mapMessages.containsKey(key) ? mapMessages.get(key) : new ArrayList<>();
                list.add(message);
                mapMessages.put(key, list);
            }
            List<Map<String, Object>> listMessageGroupBy = new ArrayList<>();
            for (List<Message> listMessage : mapMessages.values()) {
                Map<String, Object> oneSynthesis = new HashMap<>();
                oneSynthesis.put("processname", listMessage.get(0).targetProcessName);
                oneSynthesis.put("processversion", listMessage.get(0).currentProcessVersion);
                oneSynthesis.put("flowname", listMessage.get(0).targetFlowNodeName);
                oneSynthesis.put( CSTJSON_STATUS, listMessage.get(0).status.toString().toLowerCase());
                oneSynthesis.put("numberofmessages", listMessage.size());
                oneSynthesis.put("keygroup", listMessage.get(0).getKeyGroup());
                if (listMessage.get(0).catchEventType !=null)
                    oneSynthesis.put( CSTJSON_CATCHEVENTTYPE, listMessage.get(0).catchEventType.toString().toLowerCase());
                    
                
                List<Map<String,Object>> listDetails = new ArrayList<>();
                for (Message message : listMessage) {
                    Map<String, Object> oneMessage = new HashMap<>();
                    listDetails.add( oneMessage );
                    oneMessage.put( CSTJSON_CASEID, message.rootProcessInstanceId);
                    oneMessage.put( CSTJSON_MESSAGENAME, message.messageName);
                    oneMessage.put( CSTJSON_WID, message.waitingId); 
                    oneMessage.put( CSTJSON_MID,+message.messageId );
                    oneMessage.put( CSTJSON_EXPL, message.incompleteDetail.toString());
                    oneMessage.put( CSTJSON_CORRELATIONVALUES, message.getCorrelationSignature() );
                }
                // sort the detail
                MessagesFactory.sortMyList( listDetails, new String[] {CSTJSON_CASEID, CSTJSON_MESSAGENAME, CSTJSON_WID});
                
                oneSynthesis.put("details", listDetails);
                listMessageGroupBy.add(oneSynthesis);
            }
            
            // sort it
            MessagesFactory.sortMyList( listMessageGroupBy, new String[] {"processname", "processversion", "flowname"});

            result.put("listmessages", listMessageGroupBy);
            result.put("nbGroups", listMessageGroupBy.size());
            result.put(CSTJSON_NBRECONCILIATIONS, nbReconciliations);
            result.put("nbCompleteMessages", nbCompleteMessages);
            result.put("nbIncompleteMessages", nbIncompleteMessages);
            if (!listEvents.isEmpty())
                result.put( CSTJSON_LISTEVENTS, BEventFactory.getHtml(listEvents));
            
            result.put( CSTJSON_PERFORMANCEMESURE, performanceMesure.getMap());
            return result;
        }
    }

    public static class ReconcialiationFilter {

        public enum TYPEFILTER {
            MAXMESSAGES, MESSAGEKEY, GROUPKEY
        };

        public TYPEFILTER typeFilter = TYPEFILTER.MAXMESSAGES;
        public MessagesList messagesList;
        // messageList can borrow a getListKeys or a getListKeysGroups
        public boolean isKeyList = true;
        public int fromIndex;
        public int toIndex;

        public int numberOfMessages;

        /**
         * if waiting_event contains 4 records for a signatures (same message/process/target/correlation) and message_instance contains 5 records, result is 4*5 messages
         * When this factor is activated, the result will be min(waiting_event, message_event) 
         */
        public boolean reduceCrossJoint = false;
        public ReconcialiationFilter(TYPEFILTER typeFilter) {
            this.typeFilter = typeFilter;
        }

    }

    /**
     * get the list of all incomplete reconciliation messages
     * @return
     */
    public ResultMessageOperation getListIncompleteMessage(ReconcialiationFilter reconciliationFilter, ProcessAPI processAPI) {
        ResultMessageOperation result = new ResultMessageOperation();
        MessagesFactory messagesFactory = new MessagesFactory();
        
        PerformanceMesure perf = result.performanceMesure.getMesure(CSTJSON_PERFORMANCEMESURETOTAL);
        perf.start();
     
        
        // we report only one design event per triplet processname/processversion/targetflownode        
        HashSet<String> filterOnEventProcess = new HashSet<>();
        reconciliationFilter.reduceCrossJoint = true;
        ResultOperation resultReconciliation = messagesFactory.getListReconcilationMessages(reconciliationFilter);
        result.listEvents.addAll(resultReconciliation.getListEvents());
        result.performanceMesure.add( resultReconciliation.performanceMesure);
        result.nbReconciliations = resultReconciliation.getNumberOfMessages();

        Map<Long, DesignProcessDefinition> cacheDesign = new HashMap<>();
        
        for (Message message : resultReconciliation.getListMessages()) {
            message.status = enumStatus.COMPLETE;
            
            // is this message is complete ? First get the data attached to this message
            PerformanceMesure perfContent = result.performanceMesure.getMesure("messagecontentdata");
            perfContent.start();
            result.listEvents.addAll(messagesFactory.loadMessageContentData(message, true));
            perfContent.stop();
            
            PerformanceMesure perfDesign = result.performanceMesure.getMesure("messagedesign");
            perfDesign.start();
            List<BEvent> listEventDesign = messagesFactory.loadDesignContentData(message, processAPI,cacheDesign);
            perfDesign.stop();
            
            if (!listEventDesign.isEmpty()) {
                String keyEvent = message.targetProcessName + "#" + message.currentProcessVersion + "#" + message.targetFlowNodeName;
                if (!filterOnEventProcess.contains(keyEvent))
                    result.listEvents.addAll(listEventDesign);
                filterOnEventProcess.add(keyEvent);
            }
            if (! message.isDesignContentFound)
                message.status = enumStatus.FAILEDDESIGN;
            else
            {
                message.completeMessage.clear();
                // if all information are fullfill?
                message.isComplete = true;
                if (message.designContent != null && message.status != enumStatus.FAILEDDESIGN) {
    
                    message.incompleteDetail.append("DesignKey: "+message.designContent);
                    message.incompleteDetail.append(", MessageKey:");
                    boolean first=true;
                    for (String key : message.messageInstanceVariables.keySet()) {
                        if (! first)
                            message.incompleteDetail.append(",");
                        first=false;
                        message.incompleteDetail.append("[" + key + "]");
                    }
                    message.incompleteDetail.append(" MissingKey:"); 
                    for (String key : message.designContent) {
                        message.completeMessage.put(key, message.messageInstanceVariables.get(key));
                        if (!message.messageInstanceVariables.containsKey(key)) {
                            message.isComplete = false;
                            message.incompleteDetail.append("[" + key + "]");
                        }
                    }
    
                    if (message.isComplete) {
                        result.nbCompleteMessages++;
                        message.status = enumStatus.COMPLETE;
                    } else {
                        result.nbIncompleteMessages++;
                        message.status = enumStatus.INCOMPLETECONTENT;
                    }
                }
            }            
            // Add every time the message with the status
            result.listMessages.add(message);
        }
        perf.stop();

        return result;
    }

    /**
     * 
     *
     */
    public class ResultExecution {

        List<Message> listMessages = new ArrayList<>();
        List<BEvent> listEvents = new ArrayList<>();
        public int nbMessagesErrors=0;
        public int nbMessagesCorrects=0;
        public int nbDataRowDeleted=0;
        public int nbDataMessageDeleted=0;
        
        protected PerformanceMesureSet performanceMesure = new PerformanceMesureSet();
        
        
        public void addOneExecution(Message message, List<BEvent> listEvents) {

            this.listMessages.add(message);
            this.listEvents.addAll(listEvents);
        }

        public Map<String, Object> getMap() {
            Map<String,Object> result= new HashMap<>();
            result.put("messageserrors",  nbMessagesErrors);
            result.put("messagescorrects",  nbMessagesCorrects);
            result.put("nbdatarowdeleted",  nbDataRowDeleted);
            result.put("nbdatamessagedeleted",  nbDataMessageDeleted);
            result.put( CSTJSON_LISTEVENTS, BEventFactory.getHtml(listEvents));

            List<Map<String,Object>> listDetails = new ArrayList<>();

            for (Message message : listMessages) {
                Map<String, Object> oneMessage = new HashMap<>();
                listDetails.add( oneMessage );
                oneMessage.put( CSTJSON_CASEID, message.rootProcessInstanceId);
                oneMessage.put( CSTJSON_MESSAGENAME, message.messageName);
                oneMessage.put( CSTJSON_WID, message.waitingId); 
                oneMessage.put( CSTJSON_MID, message.messageId );
                oneMessage.put( CSTJSON_STATUSEXEC, message.status.toString().toLowerCase() );
                oneMessage.put( CSTJSON_NBEXECUTIONINPROGRESS,+message.nbExecutionsInProgress );
                
                oneMessage.put("expl", message.incompleteDetail.toString());
            }
            // sort the detail
            MessagesFactory.sortMyList( listDetails, new String[] {CSTJSON_CASEID, CSTJSON_MESSAGENAME, CSTJSON_WID});
            
            result.put("details", listDetails);
            result.put( CSTJSON_PERFORMANCEMESURE, performanceMesure.getMap());
            
            return result;
        }
    }

    /**
     * @return
     */
    public ResultExecution executeIncompleteMessage(MessagesList messagesList, ProcessAPI processAPI) {
        ResultExecution resultExecution = new ResultExecution();
        MessagesFactory messagesFactory = new MessagesFactory();
        PerformanceMesure perf = resultExecution.performanceMesure.getMesure(CSTJSON_PERFORMANCEMESURETOTAL);
        perf.start();
   
        // Execute per page of 100 messages
        ReconcialiationFilter reconciliationFilter = new ReconcialiationFilter(messagesList.isBorrowKeys() ? TYPEFILTER.MESSAGEKEY : TYPEFILTER.GROUPKEY);
        reconciliationFilter.fromIndex = 0;
        reconciliationFilter.messagesList = messagesList;
        int pageSize = 10;
        // we expect multiple message per result here
        reconciliationFilter.numberOfMessages= Math.max(messagesList.getNumberOfMessages(),100);
        List<String> listData = messagesList.getListKeysGroup() != null ? messagesList.getListKeysGroup() : messagesList.getListKeys();
        if (listData == null) {
            resultExecution.listEvents.add(eventSqlNoMessagesSelected);
            return resultExecution;
        }
        
        // We have to protect the Multiple message. We may have for one waiting_event MULTIPLE message_event.
        // Key is waitingEventId
        Set<String> waitingEventTreated = new HashSet<>();
        
        while (reconciliationFilter.fromIndex < listData.size()) {
            reconciliationFilter.toIndex = reconciliationFilter.fromIndex + pageSize;
            if (reconciliationFilter.toIndex > listData.size())
                reconciliationFilter.toIndex = listData.size();

            ResultMessageOperation resultOperation = getListIncompleteMessage(reconciliationFilter, processAPI);
            resultExecution.listEvents.addAll(resultOperation.listEvents);
            resultExecution.performanceMesure.add( resultOperation.performanceMesure);

            // immediately calculate the next page
            reconciliationFilter.fromIndex += pageSize;

            List<Message> listMessagesInstanceToPurge = new ArrayList<>();
            for (Message message : resultOperation.listMessages) {
                // is this message is complete ? First get the data attached to this message

                // Execute it : first, how many message_event do we have, how many waiting_event ?
                PerformanceMesure perfRelativeInformation = resultExecution.performanceMesure.getMesure("relativeinformation");
                perfRelativeInformation.start();
           
                List<BEvent> listEventsOneMessage = new ArrayList<>();
                listEventsOneMessage.addAll(messagesFactory.loadRelativeInformations(message));
                perfRelativeInformation.stop();
                
                // no sens to resend a complete message, so skip it
                if (message.isComplete)
                    continue;
                // is this waiting event was already treated ? For a message, we managed ALL instance of messages
                // example, message target=DesProcess/MessageKitchen / Key="potatoes", we have 6 waitings event and 7 message instance, then
                // we send the 6 waiting event. So, when in the list the same signature arrived again (must be the situation because in this
                // perspective, request should return 6*7 messages), we dont want to process them again.
                if (waitingEventTreated.contains(message.getSignatureMessage()))
                {
                    message.status = enumStatus.DUPLICATE;
                    resultExecution.addOneExecution(message, listEventsOneMessage);
                    continue;
                }
                waitingEventTreated.add(message.getSignatureMessage());
                // note: now send <waiting_event> message
                long minExecution = Math.min(message.nbWaitingEvent, message.nbMessageInstance);
                for (int i = 0; i < minExecution; i++) {
                    PerformanceMesure perfSendMessage = resultExecution.performanceMesure.getMesure("sendmessage");
                    perfSendMessage.start();
                    List<BEvent> listEvents = messagesFactory.executeMessage(message, message.completeMessage, processAPI);
                    perfSendMessage.stop();
                    
                    if (BEventFactory.isError(listEvents)) {
                        resultExecution.nbMessagesErrors++;
                    } else {
                        resultExecution.nbMessagesCorrects++;
                        message.nbExecutionsInProgress++;
                    }
                    listEventsOneMessage.addAll( BEventFactory.filterUnique(listEvents));
                }
                resultExecution.addOneExecution(message, listEventsOneMessage);

                // Ok, execute it, then purge all the non complete message
                // attention, purge all message_event only if the waiting_event is gone
                // let's execute all the messages, after remove that. Engine will have the time to execute it
                if (minExecution>0 && ! BEventFactory.isError(listEventsOneMessage))
                    listMessagesInstanceToPurge.add(message);

            }

            if (!listMessagesInstanceToPurge.isEmpty()) {
                
                // we want to check if the message is processed. So, wait 10 seconds to let the message be processed.
                try {
                    Thread.sleep(1000*10);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                
                // we can process the purge immediately: we took a photo BEFORE which message_instance we have to purge
                for (Message message : listMessagesInstanceToPurge) {
                    // is the waiting_event is still here ? Should be done now
                    long nbOriginalWaitingEvent = message.nbWaitingEvent;
                    messagesFactory.loadRelativeInformations(message);
                    if (message.nbWaitingEvent <= nbOriginalWaitingEvent - message.nbExecutionsInProgress) {
                        message.nbExecutionsWithSuccess = message.nbExecutionsInProgress;
                        // now we have to purge the message_event
                        ResultPurge resultPurge =  messagesFactory.purgeMessageInstance(message, messagesList.getPurgeAllRelatives(), message.nbExecutionsWithSuccess);
                        resultExecution.performanceMesure.add( resultPurge.performanceMesure );
                        
                        resultExecution.listEvents.addAll( resultPurge.listEvents);
                        resultExecution.nbDataRowDeleted += resultPurge.nbRowDataDeleted;
                        resultExecution.nbDataMessageDeleted+= resultPurge.nbRowDataDeleted;
                      
                        
                    }
                }
            }
        }
        perf.stop();
        return resultExecution;

    }

    
    

}
