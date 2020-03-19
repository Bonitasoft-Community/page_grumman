package org.bonitasoft.grumman.message;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.bonitasoft.engine.api.ProcessAPI;
import org.bonitasoft.engine.expression.Expression;
import org.bonitasoft.engine.expression.ExpressionBuilder;
import org.bonitasoft.engine.expression.ExpressionType;
import org.bonitasoft.engine.expression.InvalidExpressionException;

import org.bonitasoft.log.event.BEvent;

import lombok.Data;

// Operation Correction
// operation to correct the message
public abstract class OperationCorr {
    
    
    public @Data static class ResultOperationCorr {
        List<BEvent>  listEvents = new ArrayList<>();;
        StringBuilder explanations = new StringBuilder();
        /**
         * if no error are detected, result is empty
         */
        StringBuilder errorsDetected= new StringBuilder();
        public boolean isError() {
            return errorsDetected.length()>0;
        }
        
    }
    /** retrieve the error in the message. 
     * Note: the detection may update the message to keep track of the analysis
     * 
     * @return
     */
    public abstract ResultOperationCorr detectErrorInMessage( Message message, ProcessAPI porcessAPI);
    
    /**
     * Execute a correction on the message
     * @param message
     * @param processAPI
     * @return
     */
    public abstract ResultOperationCorr operation(Message message, ProcessAPI processAPI);
    
    
    /* -------------------------------------------------------------------- */
    /*                                                                      */
    /* Tool for operations */
    /*                                                                      */
    /* -------------------------------------------------------------------- */

    
    protected @Data class ExpressionDescription {
        protected String name;
        protected String value;
        ExpressionDescription(String name, String value ) {
            this.name = name;
            this.value = value;
        }
    }
    protected Map<Expression, Expression> createMapExpression(List<ExpressionDescription> listValues) throws InvalidExpressionException, IllegalArgumentException {
        Map<Expression, Expression> mapExpression = new HashMap<>();
        for (ExpressionDescription oneExpression : listValues) {
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

}
