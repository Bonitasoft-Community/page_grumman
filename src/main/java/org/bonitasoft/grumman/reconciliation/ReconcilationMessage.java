package org.bonitasoft.grumman.reconciliation;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.bonitasoft.engine.api.ProcessAPI;
import org.bonitasoft.grumman.message.MessagesFactory;
import org.bonitasoft.grumman.message.MessagesFactory.Message;
import org.bonitasoft.log.event.BEvent;
import org.bonitasoft.log.event.BEventFactory;


public class ReconcilationMessage {

    
    public static class ResultMessageOperation {
        public List<BEvent> listEvents = new ArrayList<>();
        public List<Message> listMessages = new ArrayList<>();
        
        public Map<String,Object> getMap() {
            Map<String,Object> result = new HashMap<>();
            result.put("listmessages", listMessages);
            result.put("nbmessage", listMessages.size());
            result.put("listevents", BEventFactory.getSyntheticHtml(listEvents));
            return result;
        }
        public Map<String,Object> getGroupByMap() {
            // result are grouped per processname/processversion/flownodename
            Map<String,Object> result = new HashMap<>();
            Map<String,List<Message>> mapMessages = new HashMap<>();
            for (Message message : listMessages) {
                String key = message.processName+"#"+message.processVersion+"#"+message.flowNodeName;
                List<Message> list = mapMessages.containsKey( key ) ? mapMessages.get(key) : new ArrayList<>();
                list.add(message);
                mapMessages.put( key, list );
            }
            List<Map<String,Object >> listMessageGroupBy = new ArrayList<>();
            for(List<Message> listMessage : mapMessages.values())
            {
                Map<String,Object> oneSynthesis = new HashMap<>();
                oneSynthesis.put("processname", listMessage.get(0).processName);
                oneSynthesis.put("processversion", listMessage.get(0).processVersion);
                oneSynthesis.put("flowname", listMessage.get(0).flowNodeName);
                oneSynthesis.put("numberofmessages", listMessage.size());
                StringBuilder details = new StringBuilder();
                for (Message message : listMessage) {
                    details.append( "("+message.messageId+": missing data "+message.incompleteDetail+");");
                }
                oneSynthesis.put("details", details.toString());
                listMessageGroupBy.add( oneSynthesis );
            }

            result.put("listmessages", listMessageGroupBy );
            return result;
        }
    }
    /**
     * 
     * @return
     */
    public ResultMessageOperation getListIncompleteMessage( int numberOfMessages, ProcessAPI processAPI) {
        ResultMessageOperation result = new ResultMessageOperation();
        MessagesFactory messagesFactory = new MessagesFactory();

        List<Message> listReconciliationMessage = messagesFactory.getListReconcilationMessages(numberOfMessages);
        for (Message message : listReconciliationMessage) {
            // is this message is complete ? First get the data attached to this message
            Map<String, Object> messageContent = messagesFactory.loadMessageContentData(message, true);
            Set<String> designContent = message.processDefinitionId==null ? null  : messagesFactory.loadDesignContentData(message.processDefinitionId, message.flowNodeName, processAPI);
            Map<String, Object> completeMessage = new HashMap<>();
            // if all information are fullfill?
            message.isComplete = true;
            if (designContent != null) {
                
                for (String key : designContent) {
                    completeMessage.put(key, messageContent.get(key));
                    if (!messageContent.containsKey(key)) {
                        message.isComplete = false;
                        message.incompleteDetail.append("["+key+"]");
                    }
                }
    
                if (!message.isComplete) {
                    message.completeMessage = completeMessage;
                    result.listMessages.add( message );
                }
            }
        }
        return result;
    }
    
    /**
     * 
     *
     */
    public class ResultExecution {

        List<Message> listMessages = new ArrayList<>();

        public void addOneExecution(Message message, List<BEvent> listEvents) {

            listMessages.add(message);
        }
        public Map<String,Object> getMap() {
            return new HashMap<>();
        }
    }

    /**
     * 
     * @return
     */
    public ResultExecution executeIncompleteMessage( int numberOfMessages, ProcessAPI processAPI) {
        ResultExecution resultExecution = new ResultExecution();
        MessagesFactory messagesFactory = new MessagesFactory();
        ResultMessageOperation resultOperation = getListIncompleteMessage(numberOfMessages, processAPI);
        List<Message> listMessagesInstanceToPurge = new ArrayList<>();
        for (Message message : resultOperation.listMessages) {
            // is this message is complete ? First get the data attached to this message

            // Execute it : first, how many message_event do we have, how many waiting_event ?
            messagesFactory.loadNumberOfRecords(message);
            int minExecution = Math.min(message.nbWaitingEvent, message.nbMessageContent);
            List<BEvent> listAllEvents= new ArrayList<>();
            for (int i = 0; i < minExecution; i++) {
                List<BEvent> listEvents = messagesFactory.executeMessage(message, message.completeMessage);
                message.nbExecutionsInProgress = BEventFactory.isError(listEvents) ? 0 : 1;
                listAllEvents.addAll(listEvents);
            }
            resultExecution.addOneExecution(message, listAllEvents);

            // Ok, execute it, then purge all the non complete message
            // attention, purge all message_event only if the waiting_event is gone
            // let's execute all the messages, after remove that. Engine will have the time to execute it
            listMessagesInstanceToPurge.add(message);

        }

        if (!listMessagesInstanceToPurge.isEmpty()) {
            // give 30 s more to the server to execute all informations
            try {
                Thread.sleep(1000 * 30);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            for (Message message : listMessagesInstanceToPurge) {
                // is the waiting_event is still here ? Should be done now
                long nbOriginalWaitingEvent = message.nbWaitingEvent;
                messagesFactory.loadNumberOfRecords(message);
                if (message.nbWaitingEvent <= nbOriginalWaitingEvent - message.nbExecutionsInProgress) {
                    message.nbExecutionsWithSuccess = message.nbExecutionsInProgress;
                    // now we have to purge the message_event
                    messagesFactory.purgeMessageInstance(message, message.nbExecutionsWithSuccess);
                }
            }
        }

        return resultExecution;

    }

}
