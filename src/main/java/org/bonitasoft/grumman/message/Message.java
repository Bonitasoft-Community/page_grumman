package org.bonitasoft.grumman.message;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.logging.Logger;

import org.bonitasoft.engine.api.ProcessAPI;
import org.bonitasoft.engine.bpm.flownode.CorrelationDefinition;
import org.bonitasoft.engine.bpm.flownode.SendEventException;
import org.bonitasoft.engine.expression.Expression;
import org.bonitasoft.engine.expression.ExpressionBuilder;
import org.bonitasoft.engine.expression.ExpressionType;
import org.bonitasoft.engine.expression.InvalidExpressionException;
import org.bonitasoft.engine.operation.Operation;
import org.bonitasoft.log.event.BEvent;
import org.bonitasoft.log.event.BEvent.Level;



public class Message {

    private final static Logger logger = Logger.getLogger(Message.class.getName());

    private static String loggerLabel = "MessagesFactory ##";
    private final static BEvent eventSendMessageError = new BEvent(Message.class.getName(), 1, Level.ERROR, "Send error", "Error when a message is send", "Message is not sended", "Check error");

    /**
     *  a message containts:
     *  - information in message_instance (message content waiting for a correlation)
     *  - information in waiting_event (message event waiting to have a message content).
     *  According the information carry on, the waitingEvent and/or the messageinstance can be fullfill
     *  When waitingEvent AND messageInstance are complete, that mean the two informations match on the correlation key (both have the same correlation key).
     *    isMessageWithCorrelation is true
     *  
     *  
     */
    
    /**
     * INCOMPLETECONTENT : the message does not have a complete content to be executed.
     * FAILEDDESIGN: the target process can't be loaded, or does not contains the target flow node
     * COMPLETE : the message is complete. So an another error should arrived during the execution.
     * DUPLICATE : if there are 2 message_instance for 1 waiting_event, there are then 2 messages. In this situation, we will send only 1 message.
     * Another message is sent as DUPLICATE. Note : 5 messageInstance / 3 waitingevent = 15 messages. Send only 1 message, but this message send <waitingevent> BPMN messages and mark another as DUPLICATED.
     * SENDED : message sent with success
     * @author Firstname Lastname
     *
     */
    public enum enumStatus { INCOMPLETECONTENT, FAILEDDESIGN, COMPLETE, DUPLICATE, SENDED, SENDFAILED };
    public enumStatus status;
    /**
     * theses informations describe the message
     */
    public String messageName;
    
    public String targetProcessName;
    public String targetFlowNodeName;
    
    
    /**
     * waiting event part 
     */
    
    
    /**
     * message don't store the process version. So, this version is calculated on the current available process on server, according the waiting event
     */
    public String currentProcessVersion;
    /**
     * Message 
     */
    public Long processDefinitionId;
    
    public String rootprocessName;
    public String rootprocessVersion;
    public Long rootProcessDefinitionId;

    public Map<String, Object> waitingEvent = new HashMap<>();
    public Long waitingId;
    public Long rootProcessInstanceId;
    public boolean isDesignContentFound=false;
    public enum enumCatchEventType { STARTMESSAGE, TASKMESSAGE, BOUNDARYEVENT, CATCHMESSAGEEVENT, SUBPROCESSEVENT };
    public enumCatchEventType catchEventType;
    public Set<String> designContent = new HashSet<>();
    public Long dateWaitingEvent;
    public long nbWaitingEvent;

    
    /**
     * message instance part
     */
    public Map<String, Object> messageInstance = new HashMap<>();
    public Long messageId;
    
    public long nbMessageInstance;
    /**
     * before send the message, take a picture of all messageinstances to be deleted
     */
    public List<Long> listIdMessageInstanceRelative = new ArrayList<Long>();
    public Map<String, Object> messageInstanceVariables = new HashMap<>();

    /**
     * common part : the message may be with a correlation (exist both in waitingevent and messageinstance), then here the correlation part
     */
    /**
     * true if this message contains a waitingEvent AND a messageinstance
     */
    public boolean isMessageWithCorrelation = true;
    public List<CorrelationDefinition> listCorrelationDefinitions=null;
    public List<Operation> listOperations = new ArrayList<Operation>();
    
    /**
     * when the message is incomplete, this is the complete one
     */
    public Map<String, Object> completeMessage = new HashMap<String,Object>();
    public boolean isComplete = true;

    public StringBuilder incompleteDetail= new StringBuilder();
    
    /**
     * information about the management of the message.
     */
    public int nbExecutionsInProgress = 0;
    public int nbExecutionsWithSuccess = 0;

    public String toString() {
        return targetProcessName + "(" + currentProcessVersion + ")-" + targetFlowNodeName + ", waitingId"+waitingId+", messageId"+messageId+" date:" + dateWaitingEvent + ", nbWaitingEvent:" + nbWaitingEvent + " nbMessageContent:" + nbMessageInstance;
    }
    
    /**
     * in case of a Reconciliation Message, the ID is the waiting_event.id
     * @return
     */
    public Long getReconciliationMessageId() {
        return (Long) waitingEvent.get("id");
    }
    
    /**
     * column name are indentical in tables waiting_event and message_instance
     */
    public static final String[] listColumnCorrelation = { "correlation1","correlation2","correlation3","correlation4","correlation5"};
    
    public Object getValueCorrelation( int i, boolean keepNone) {
        Object value=null;
        if (isMessageWithCorrelation) {
            if (waitingEvent!=null)
                value=waitingEvent.get(listColumnCorrelation[ i ]);
        }
        else {
          if (waitingEvent!=null)
              value=waitingEvent.get(listColumnCorrelation[ i ]);
          if (messageInstance !=null)
              value=messageInstance.get(listColumnCorrelation[ i ]);
        }
      if (keepNone)
          return value;
      return (value==null || "NONE".equals(value)) ? null : value;
    }
    
    /**
     * correlation name information
     * @param correlations
     */
    public void setCorrelations( List<CorrelationDefinition> correlations) {
        listCorrelationDefinitions = correlations;
    }
    public String getNameCorrelation( int i) {
        if (listCorrelationDefinitions == null || i >= listCorrelationDefinitions.size())
            return null;
        return listCorrelationDefinitions.get( i ).getKey().getName();
      }
    /**
     * return a signature for the correlation values
     * @return
     */
    public String getCorrelationSignature() {
        Object[] correlationValue = new Object[ listColumnCorrelation.length];
        for (int i=0;i<listColumnCorrelation.length;i++)
        {
            correlationValue[ i ] = getValueCorrelation( i,false);
        }
        return getCorrelationSignature( correlationValue );
    }
    /**
     * static to share it with different source
     * @param correlationValue
     * @return
     */
    public static String getCorrelationSignature( Object[] correlationValue ) {
        StringBuilder signature = new StringBuilder();
        for (int i=0;i<correlationValue.length;i++)
        {
            if (i>0) {
                signature.append( "," );
            }
            Object valueCorrelation = correlationValue[ i ];            
            signature.append( valueCorrelation==null? "[]":"["+valueCorrelation.toString()+"]");
        }
        return signature.toString();

    }
    
    
    
    public void addListOperations(List<Operation>listOperations) {
        this.listOperations.addAll( listOperations);
        
    }
      
    /* -------------------------------------------------------------------- */
    /*                                                                      */
    /* Send message */
    /*                                                                      */
    /* -------------------------------------------------------------------- */
 
    public List<BEvent> sendMessage(ProcessAPI processAPI) {
        List<BEvent> listEvents = new ArrayList<>();
        try {

            Expression targetProcess = new ExpressionBuilder().createConstantStringExpression(targetProcessName);
            Expression targetFlowNode = new ExpressionBuilder().createConstantStringExpression(targetFlowNodeName);
            List<ExpressionDescription> listExpressions = new ArrayList<>();
            if (completeMessage!=null) {
                for (Entry<String, Object> entry : completeMessage.entrySet()) {
                    listExpressions.add( new ExpressionDescription(entry.getKey(), entry.getValue()==null ? null : entry.getValue().toString() ));
                }
            }
            Map<Expression, Expression> messageContent = createMapExpression(listExpressions);

            if (isMessageWithCorrelation && listCorrelationDefinitions!=null) {
                listExpressions = new ArrayList<>();


                StringBuilder traceCorrelation = new StringBuilder();
                
                
                // we have to attached the correct key.
                Map<String,Object> mapCorrelationValues = new HashMap<>();
                for (int i=0;i<listColumnCorrelation.length;i++) {
                    Object value= getValueCorrelation( i,false);
                    if (value!=null) {
                        //  format is keyId-$-1003
                        int pos = value.toString().indexOf("-$-");
                        if (pos!=-1) {
                            mapCorrelationValues.put(value.toString().substring(0,pos), value.toString().substring(pos+3));
                        }
                    }
                }
                    
                for (int i=0;i<listCorrelationDefinitions.size();i++)
                {
                    CorrelationDefinition correlation = listCorrelationDefinitions.get(i);
                    Object value = mapCorrelationValues.get(correlation.getKey().getName());
                    listExpressions.add( new ExpressionDescription(correlation.getKey().getName(), value==null ? null: value.toString()));
                    traceCorrelation.append("["+correlation.getKey().getName()+"]=["+(value==null ? null: value.toString())+"], ");
                }
                    
                Map<Expression, Expression> messageCorrelations = createMapExpression(listExpressions);
                logger.info( loggerLabel+" Send message["+messageName+"] targetProcess["+targetProcessName+"] FlowName["+targetFlowNodeName+"] RootCaseId["+rootProcessInstanceId+"]"+traceCorrelation);
                processAPI.sendMessage(messageName, targetProcess, targetFlowNode, messageContent, messageCorrelations);
                
                
            } else {
                processAPI.sendMessage( messageName, targetProcess, targetFlowNode, messageContent);
            }
            status=enumStatus.SENDED;            
        } catch (SendEventException se) {
            status=enumStatus.SENDFAILED;            

            listEvents.add(new BEvent(eventSendMessageError, se, messageName+" e:"+se.toString()));
        } catch (Exception e) {
            listEvents.add(new BEvent(eventSendMessageError, e, messageName+" e:"+e.toString()));
            status=enumStatus.SENDFAILED;            
        }
       
        return listEvents;
    }

    
    private class ExpressionDescription {
        protected String name;
        protected String value;
        ExpressionDescription(String name, String value ) {
            this.name = name;
            this.value = value;
        }
    }
    private Map<Expression, Expression> createMapExpression(List<ExpressionDescription> listValues) throws InvalidExpressionException, IllegalArgumentException {
        Map<Expression, Expression> mapExpression = new HashMap<Expression, Expression>();
        for (ExpressionDescription oneExpression : listValues) {

            //Expression exprName= new ExpressionBuilder().createNewInstance("name"+name).setContent( name).setExpressionType( ExpressionType.TYPE_CONSTANT).setReturnType( String.class.getName()).done();
            //Expression exprValue= new ExpressionBuilder().createNewInstance("value").setContent( value ).setExpressionType( ExpressionType.TYPE_CONSTANT).setReturnType(String.class.getName()).done();

            Expression exprName = new ExpressionBuilder().createConstantStringExpression(oneExpression.name);
            Expression exprValue = null;
            if (oneExpression.value == null || oneExpression.value.length() == 0)
                exprValue = new ExpressionBuilder().createNewInstance("value-" + oneExpression.name).setContent("").setExpressionType(ExpressionType.TYPE_CONSTANT).setReturnType(String.class.getName()).done();
            else
                exprValue = new ExpressionBuilder().createConstantStringExpression(oneExpression.value);
            mapExpression.put(exprName, exprValue);
        }
        return mapExpression;
    }

    /* -------------------------------------------------------------------- */
    /*                                                                      */
    /* Send message */
    /*                                                                      */
    /* -------------------------------------------------------------------- */
    /**
     * A key to determine a message.
     * In database, we may have multiple message with the same "signature". The signature is composed of
     * - the target process, target flownode
     * - same correlations key
     * If multiple messages are send with the same correlations keys, they have the same signature.

     * @return
     */
    public String getSignatureMessage() {
        StringBuilder signature = new StringBuilder();
        signature.append( messageName );
        signature.append( "#" );
        signature.append( targetProcessName );
        signature.append( "#" );
        signature.append( targetFlowNodeName );
        for (int i=0;i<listColumnCorrelation.length;i++)
        {
            signature.append( "#" );
            Object valueCorrelation =getValueCorrelation( i,true);            
            signature.append( valueCorrelation==null? "null":valueCorrelation.toString());
        }
        
        return signature.toString();
    }
 
        /* -------------------------------------------------------------------- */
        /*                                                                      */
        /* MessageKeyGroup */
        /*                                                                      */
        /* -------------------------------------------------------------------- */
     
    /**
     * MessageKeyGroup structure
     *
     */
    public static class MessageKeyGroup {
        
        public String processName;
        public String processVersion;
        public String flowNodeName;
        public static MessageKeyGroup getInstanceFromMessage( Message message ) 
        {
            MessageKeyGroup messageKeyGroup = new MessageKeyGroup();
            messageKeyGroup.processName     = message.targetProcessName;
            messageKeyGroup.processVersion  = message.currentProcessVersion;
            messageKeyGroup.flowNodeName    = message.targetFlowNodeName;
            return messageKeyGroup;
        }
        public static MessageKeyGroup getInstanceFromKey( String key )
        {
            MessageKeyGroup messageKeyGroup = new MessageKeyGroup();
            StringTokenizer st = new StringTokenizer( key, "#");
            messageKeyGroup.processName     = st.hasMoreTokens()? st.nextToken() : null;
            messageKeyGroup.processVersion  = st.hasMoreTokens()? st.nextToken() : null;
            messageKeyGroup.flowNodeName    = st.hasMoreTokens()? st.nextToken() : null;
            return messageKeyGroup;            
        }

        public String getKey() {
            return processName+"#"+processVersion+"#"+flowNodeName;
        }
    }
    
    
    
    public String getKeyGroup() {
        return MessageKeyGroup.getInstanceFromMessage( this ).getKey();
    }
    
    
    public Map<String,Object> getMap() {
        Map<String,Object> result = new HashMap<>();
        result.put("event",  waitingEvent);
        result.put("message",  messageInstance);
        return result;
    }
}