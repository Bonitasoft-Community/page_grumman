package org.bonitasoft.grumman.performance;

import java.util.HashMap;
import java.util.Map;

/**
 * register a performance set
 *
 */
public class PerformanceMesureSet {
    
    public class PerformanceMesure {
        String name;
        long currentTimeStart=0;
        long sumTimeExecution=0;
        int nbExecutions=0;
        
        public PerformanceMesure ( String name ) {
            this.name=name;
        }
        public void start() {
            currentTimeStart = System.currentTimeMillis();
        }
        public void stop() {
            sumTimeExecution = System.currentTimeMillis() - currentTimeStart;
            nbExecutions++;
        }
        public long getSumTimeExecution() {
            return sumTimeExecution; 
        }

        public long getNbExecution() {
            return nbExecutions; 
        }

    }
    public Map<String,PerformanceMesure> mapPerformanceMesure = new HashMap<>();
    
    public PerformanceMesure getMesure( String name ) {
     return mapPerformanceMesure.computeIfAbsent(name, k-> new PerformanceMesure(name));
    }
    
    /**
     * Add the perf set. if the performance does not exists, add it, else merge it
     * @param perfMesureSet
     */
    public void add( PerformanceMesureSet perfMesureSet ) {
        for (PerformanceMesure perf : perfMesureSet.mapPerformanceMesure.values())
        {
            PerformanceMesure existingPerf = mapPerformanceMesure.get( perf.name);
            if (existingPerf==null)
                mapPerformanceMesure.put(perf.name, perf);
            else {
                existingPerf.sumTimeExecution += perf.sumTimeExecution;
                existingPerf.nbExecutions += perf.nbExecutions;
            }
        }
    }
    
    /**
     * get the map of all entry
     * @return
     */
    public Map<String, Object> getMap() {
        Map<String,Object> result= new HashMap<>();
        for (java.util.Map.Entry<String,PerformanceMesure> entry : mapPerformanceMesure.entrySet())
        {
            Map<String,Object> resultMesure= new HashMap<>();
            resultMesure.put("time", entry.getValue().getSumTimeExecution());
            resultMesure.put("nb", entry.getValue().getNbExecution());
            
            result.put(entry.getKey(),  resultMesure);
        }
        return result;
    }

}
