package org.bonitasoft.grumman.message;

import java.io.Serializable;
import java.util.logging.Logger;

import org.bonitasoft.engine.api.ProcessAPI;
import org.bonitasoft.engine.bpm.data.DataInstance;
import org.bonitasoft.engine.bpm.data.DataNotFoundException;
import org.bonitasoft.engine.operation.Operation;
import org.bonitasoft.grumman.message.Message.enumStatus;
import org.bonitasoft.log.event.BEvent;
import org.bonitasoft.log.event.BEvent.Level;

public class OperationCorrExecuteMessage extends OperationCorr {

    private final static Logger logger = Logger.getLogger(OperationCorrExecuteMessage.class.getName());

    private static String loggerLabel = "OperationCorrExecuteMessage ##";
    private final static BEvent eventExecuteEventError = new BEvent(OperationCorrExecuteMessage.class.getName(), 1, Level.ERROR, "Execute error", "Error when a Event node is executed", "Event is still waiting", "Check error");

    @Override
    public ResultOperationCorr detectErrorInMessage( Message message, ProcessAPI processAPI) {
        return executeOperation( false, message, processAPI);
        
    }

        
    /* -------------------------------------------------------------------- */
    /*                                                                      */
    /* Send message */
    /*                                                                      */
    /* -------------------------------------------------------------------- */
    @Override
    public ResultOperationCorr operation(Message message, ProcessAPI processAPI) {
        return executeOperation( true, message, processAPI);

    
    }
    
    
    private ResultOperationCorr executeOperation( boolean doTheUpdate, Message message, ProcessAPI processAPI)
    {
        ResultOperationCorr resultOperation = new ResultOperationCorr();
        // Execute all operations
        int count = 0;
        resultOperation.explanations.append( message.getListOperations().size()+" operations detected;");
        for (Operation operation : message.getListOperations()) {
            count++;
            String labelOperation = " **** Operation " + count + " ";
            String dataName = operation.getLeftOperand() != null ? operation.getLeftOperand().getName() : null;
            String sourceName = operation.getRightOperand() != null ? operation.getRightOperand().getContent() : null;
            resultOperation.explanations.append( labelOperation+" LeftOperand=["+dataName+"] RigthOperand["+sourceName+"];");
            
            if (dataName == null || dataName.length() == 0 || sourceName == null || sourceName.length() == 0) {
                resultOperation.errorsDetected.append(labelOperation + " Invalid operation LeftOperand[" + dataName + "] RightOperand[" + sourceName + "],");
                continue;
            }
            Object sourceValue = message.getMessageInstanceVariables().get(sourceName);
            boolean dataFound = false;
            try {
                // first, check if the variable is here, to get a correct error during update
                DataInstance dataInstance = processAPI.getProcessDataInstance(dataName, message.getProcessInstanceId());
                if (sourceValue !=null && ! dataInstance.getClassName().equals(sourceValue.getClass().getName()))
                    resultOperation.errorsDetected.append(labelOperation +" VariableName["+dataName+"] DataClass["+dataInstance.getClassName()+"] MessageDataClass["+sourceValue.getClass().getName()+"]");
                // update a Process Variable 
                if (doTheUpdate)
                    processAPI.updateProcessDataInstance(dataName, message.getProcessInstanceId(), (Serializable) sourceValue);
                dataFound = true;
            } catch (DataNotFoundException nfe) {
            } catch (Exception e) {
                dataFound=true;
                resultOperation.errorsDetected.append(labelOperation + " VariableName[" + dataName + "] : " + e.getMessage());
            }

            try {
                // Update a Activity variable 
                if (!dataFound) {
                    // first, check if the variable is here, to get a correct error during update
                    DataInstance dataInstance = processAPI.getActivityDataInstance(dataName, message.getFlowNodeInstanceId());
                    if (sourceValue !=null && ! dataInstance.getClassName().equals(sourceValue.getClass().getName()))
                        resultOperation.errorsDetected.append(labelOperation +" VariableName["+dataName+"] DataClass["+dataInstance.getClassName()+"] MessageDataClass["+sourceValue.getClass().getName()+"]");
                    
                    if (doTheUpdate)
                        processAPI.updateActivityDataInstance(dataName, message.getFlowNodeInstanceId(), (Serializable) sourceValue);
                    dataFound = true;
                }
            } catch (DataNotFoundException nfe) {
            } catch (Exception e) {
                dataFound=true;
                resultOperation.errorsDetected.append(labelOperation + " VariableName[" + dataName + "] : " + e.getMessage());
            }
            if (!dataFound)
                resultOperation.errorsDetected.append(labelOperation + " VariableNotFound[" + dataName + "] - type[" + operation.getLeftOperand().getType() + "],");
        }

        // Now, execute the node
        try {
            if (doTheUpdate) {
                processAPI.executeFlowNode(message.getFlowNodeInstanceId());
                message.setStatus(enumStatus.EXECUTED);
            }
        } catch (Exception e) {
            logger.severe( loggerLabel +  "Error [" + e.getMessage() + "] CaseId[" + message.getRootProcessInstanceId() + "] Message[" + message.getMessageName() + "]");
            resultOperation.listEvents.add(new BEvent(eventExecuteEventError, e, "Error [" + e.getMessage() + "] CaseId[" + message.getRootProcessInstanceId() + "] Message[" + message.getMessageName() + "]"));
            resultOperation.errorsDetected.append("Execution [" + message.getFlowNodeInstanceId() + "] failed " + e.getMessage());
            message.setStatus(enumStatus.EXECUTEDFAILED);

        }

        return resultOperation;
    }
}
