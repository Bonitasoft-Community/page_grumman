package org.bonitasoft.grumman.message;

import java.io.Serializable;
import java.util.logging.Logger;

import org.bonitasoft.engine.api.ProcessAPI;
import org.bonitasoft.engine.operation.Operation;
import org.bonitasoft.grumman.message.Message.enumStatus;
import org.bonitasoft.log.event.BEvent;
import org.bonitasoft.log.event.BEvent.Level;

import lombok.Data;

public @Data class OperationCorrExecuteMessage extends OperationCorr {

    private final static Logger logger = Logger.getLogger(OperationCorrExecuteMessage.class.getName());

    private static String loggerLabel = "OperationCorrExecuteMessage ##";
    private final static BEvent eventExecuteEventError = new BEvent(OperationCorrExecuteMessage.class.getName(), 1, Level.ERROR, "Execute error", "Error when a Event node is executed", "Event is still waiting", "Check error");
    
    

    /* -------------------------------------------------------------------- */
    /*                                                                      */
    /* Send message */
    /*                                                                      */
    /* -------------------------------------------------------------------- */
    @Override
    public ResultOperationCorr operation(Message message, ProcessAPI processAPI) {
        ResultOperationCorr resultOperation = new ResultOperationCorr();
        // Execute all operations
        int count=0;
        for (Operation operation : message.getListOperations() ) {
            count++;
            String dataName = operation.getLeftOperand()!=null ? operation.getLeftOperand().getName() : null;
            String sourceName = operation.getRightOperand() !=null ? operation.getRightOperand().getContent() : null;
            
            if (dataName ==null || dataName.length()==0 || sourceName == null || sourceName.length()==0) {
                resultOperation.explanations.append("Operation "+count+" Invalid operation LeftOperand["+dataName+"] RighOperand["+sourceName+"],");
                continue;
            }  
            Object sourceValue = message.getMessageInstanceVariables().get(sourceName);
            boolean dataUpdated=false;
            try {
                // update a Process Variable 
                processAPI.updateProcessDataInstance(dataName, message.getProcessInstanceId(), (Serializable) sourceValue);
                dataUpdated=true;
            }
            catch( Exception e) {                
            }
            
            try {
                // Update a Activity variable 
                if (!dataUpdated) {
                    processAPI.updateActivityDataInstance(dataName, message.getFlowNodeInstanceId(),(Serializable) sourceValue);
                    dataUpdated=true;
                }
            }
            catch( Exception e) {                
            }
            if (!dataUpdated)
                resultOperation.explanations.append("Operation "+count+" VariableNotFound["+dataName+"] - type["+operation.getLeftOperand().getType()+"],");
        }
        
        // Now, execute the node
        try {
            processAPI.executeFlowNode(message.getFlowNodeInstanceId());
            message.setStatus( enumStatus.EXECUTED );            

        }
        catch( Exception e) {
            resultOperation.listEvents.add( new BEvent(eventExecuteEventError, e, "Error ["+e.getMessage()+"] CaseId["+message.getRootProcessInstanceId()+"] Message["+message.getMessageName()+"]"));
            resultOperation.explanations.append("Execution ["+message.getFlowNodeInstanceId()+"] failed "+e.getMessage());
            message.setStatus( enumStatus.EXECUTEDFAILED );            

        }
        
        return resultOperation;
    }
}