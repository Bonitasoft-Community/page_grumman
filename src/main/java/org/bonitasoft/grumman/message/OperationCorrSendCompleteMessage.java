package org.bonitasoft.grumman.message;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.logging.Logger;

import org.bonitasoft.engine.api.ProcessAPI;
import org.bonitasoft.engine.bpm.flownode.CorrelationDefinition;
import org.bonitasoft.engine.bpm.flownode.SendEventException;
import org.bonitasoft.engine.expression.Expression;
import org.bonitasoft.engine.expression.ExpressionBuilder;
import org.bonitasoft.grumman.message.Message.enumStatus;
import org.bonitasoft.log.event.BEvent;
import org.bonitasoft.log.event.BEvent.Level;

public class OperationCorrSendCompleteMessage extends OperationCorr {

    private final static Logger logger = Logger.getLogger(OperationCorrSendCompleteMessage.class.getName());

    private static String loggerLabel = "OperationSendCompleteMessage ##";
    private final static BEvent eventSendMessageError = new BEvent(OperationCorrSendCompleteMessage.class.getName(), 1, Level.ERROR, "Send error", "Error when a message is send", "Message is not sended", "Check error");
    
    @Override
    public ResultOperationCorr detectErrorInMessage( Message message, ProcessAPI processAPI) {
        ResultOperationCorr resultOperation = new ResultOperationCorr();
        StringBuilder resultDetection = new StringBuilder();
        boolean errorDetected=false;
        resultDetection.append("DesignKey: " + message.designContent);
        resultDetection.append(", MessageKey:");
        boolean first = true;
        for (String key : message.messageInstanceVariables.keySet()) {
            if (!first)
                resultDetection.append(",");
            first = false;
            resultDetection.append("[" + key + "]");
        }
        resultDetection.append(" MissingKey:");
        for (String key : message.designContent) {
            message.completeMessage.put(key, message.messageInstanceVariables.get(key));
            if (!message.messageInstanceVariables.containsKey(key)) {
                errorDetected = true;
                resultDetection.append("[" + key + "]");
            }
        }
        if (errorDetected)
            resultOperation.errorsDetected = resultDetection;
        else
            resultOperation.explanations = resultDetection;
        return resultOperation;
    }
    /* -------------------------------------------------------------------- */
    /*                                                                      */
    /* Send message */
    /*                                                                      */
    /* -------------------------------------------------------------------- */
    @Override
    public ResultOperationCorr operation(Message message, ProcessAPI processAPI) {
        ResultOperationCorr resultOperation = new ResultOperationCorr();
        try {

            Expression targetProcess = new ExpressionBuilder().createConstantStringExpression(message.getTargetProcessName());
            Expression targetFlowNode = new ExpressionBuilder().createConstantStringExpression(message.getTargetFlowNodeName());
            List<ExpressionDescription> listExpressions = new ArrayList<>();
            if (message.completeMessage!=null) {
                for (Entry<String, Object> entry : message.completeMessage.entrySet()) {
                    listExpressions.add( new ExpressionDescription(entry.getKey(), entry.getValue()==null ? null : entry.getValue().toString() ));
                }
            }
            Map<Expression, Expression> messageContent = createMapExpression(listExpressions);

            if ( message.isMessageWithCorrelation &&  message.listCorrelationDefinitions!=null) {
                listExpressions = new ArrayList<>();


                StringBuilder traceCorrelation = new StringBuilder();
                
                
                // we have to attached the correct key.
                Map<String,Object> mapCorrelationValues = new HashMap<>();
                for (int i=0;i< Message.listColumnCorrelation.length;i++) {
                    Object value=  message.getValueCorrelation( i,false);
                    if (value!=null) {
                        //  format is keyId-$-1003
                        int pos = value.toString().indexOf("-$-");
                        if (pos!=-1) {
                            mapCorrelationValues.put(value.toString().substring(0,pos), value.toString().substring(pos+3));
                        }
                    }
                }
                    
                for (int i=0;i< message.listCorrelationDefinitions.size();i++)
                {
                    CorrelationDefinition correlation =  message.listCorrelationDefinitions.get(i);
                    Object value = mapCorrelationValues.get(correlation.getKey().getName());
                    listExpressions.add( new ExpressionDescription(correlation.getKey().getName(), value==null ? null: value.toString()));
                    traceCorrelation.append("["+correlation.getKey().getName()+"]=["+(value==null ? null: value.toString())+"], ");
                }
                    
                Map<Expression, Expression> messageCorrelations = createMapExpression(listExpressions);
                logger.info( loggerLabel+" Send message["+ message.getMessageName()+"] targetProcess["+ message.getTargetProcessName()+"] FlowName["+ message.getTargetFlowNodeName()+"] RootCaseId["+ message.rootProcessInstanceId+"]"+traceCorrelation);
                processAPI.sendMessage( message.getMessageName(), targetProcess, targetFlowNode, messageContent, messageCorrelations);
                
                
            } else {
                processAPI.sendMessage(  message.getMessageName(), targetProcess, targetFlowNode, messageContent);
            }
            message.setStatus( enumStatus.SENDED );            
        } catch (SendEventException se) {
            message.setStatus( enumStatus.SENDFAILED );            

            resultOperation.listEvents.add(new BEvent(eventSendMessageError, se,  message.getMessageName()+" e:"+se.toString()));
        } catch (Exception e) {
            resultOperation.listEvents.add(new BEvent(eventSendMessageError, e,  message.getMessageName()+" e:"+e.toString()));
            message.setStatus( enumStatus.SENDFAILED );            
        }
       
        return resultOperation;
    }

    
}
