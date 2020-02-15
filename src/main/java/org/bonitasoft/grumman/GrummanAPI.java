package org.bonitasoft.grumman;

import java.util.Map;

import org.bonitasoft.engine.api.ProcessAPI;
import org.bonitasoft.grumman.message.MessagesFactory;
import org.bonitasoft.grumman.reconciliation.ReconcilationMessage;

public class GrummanAPI {
    
    ProcessAPI processAPI;
    MessagesFactory messageFactory;
    public GrummanAPI(ProcessAPI processAPI) {
        this.processAPI = processAPI;
        this.messageFactory = new MessagesFactory();
    }

    
    public Map<String,Object> getSynthesis() {
        return messageFactory.getSynthesis().getMap();
    }
    
 
    /* -------------------------------------------------------------------- */
    /*                                                                      */
    /* Reconciliation */
    /*                                                                      */
    /* -------------------------------------------------------------------- */
    
    public Map<String,Object> getIncompleteReconciliationMessage(int numberOfMessages, ProcessAPI processAPI) {
        ReconcilationMessage reconciliation = new ReconcilationMessage();
        return reconciliation.getListIncompleteMessage(numberOfMessages, processAPI).getGroupByMap();
        
    }

    /**
     * A message is send, but can't be executed because all the variables are not fullfilled.
     * So, 
     * 1/ all messages reconcialed are checked
     * 2/ if a message does not contains all mandatory field, a new message is created, with all mandatory field
     * 3/ all messages waiting with not all the mandatory fields are purged
     * @param processAPI
     * @return
     */
    public  Map<String,Object> sendMessageReconcialiation(int numberOfMessages, ProcessAPI processAPI) {
        ReconcilationMessage reconciliation = new ReconcilationMessage();
        return reconciliation.executeIncompleteMessage( numberOfMessages, processAPI).getMap();
        
    }

    
    // status :
    // nombre de waiting event, de message instance
    // nombre de reconciliation
    // waiting_event le plus ancien, message_instance le plus ancien
    // repartition par processus
    // message.handle: en cours de traitement (flague pour un work)
    // waiting_event.progress => Same 
    
}
