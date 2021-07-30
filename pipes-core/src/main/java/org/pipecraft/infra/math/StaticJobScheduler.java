package org.pipecraft.infra.math;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;
import java.util.function.Function;

/**
 * Schedules n "jobs" among m "workers", trying to minimize the makespan.
 * In this class we assume a simple version of the problem: 
 * - Jobs have weights known in advance
 * - Workers are homogeneous
 * - There are no job dependencies or assignment restrictions
 * 
 * Since the problem is NP-complete, we use a simple approximation - LPT 
 * (See https://en.wikipedia.org/wiki/Multiprocessor_scheduling#Algorithms).
 * This algorithm runs in O(m*log(m)) time and achieves (4/3 - 1/(3*m)) approximation factor,
 * i.e. it is never more than 33% above the optimal result.
 * 
 * @param <J> the job type
 * 
 * @author Eyal Schneider
 */
public class StaticJobScheduler <J> {
  private final List<J> jobs;
  private final Function<J, Double> weightFunction;
  
  /**
   * Constructor
   * 
   * @param jobs The set of jobs to schedule
   * @param weightFunction A function specifying how to extract the weight of a given job.
   * Must be a valid, consistent function, returning non-negative values.
   */
  public StaticJobScheduler(Collection<J> jobs, Function<J, Double> weightFunction) {
    this.jobs = new ArrayList<>(jobs);
    this.weightFunction = weightFunction;
    this.jobs.sort(Comparator.comparing(weightFunction).reversed());
  }

  /**
   * Performs the job scheduling, returning the full assignment of jobs to workers.
   * @param workerCount The number of workers
   * @return A partitioning of the jobs given in the constructor into the workerCount workers.
   * Item at index i contains all jobs assigned to worker i. The numbering of the workers is arbitrary, but consistent.
   * Each job collection is guaranteed to be ordered in descending job weight order (when iterating it).
   */
  public List<Collection<J>> schedule(int workerCount) {
    ArrayList<Collection<J>> partition = new ArrayList<>(workerCount);
    
    PriorityQueue<Worker> loads = new PriorityQueue<>();
    for (int w = 0; w < workerCount; w++) {
      loads.add(new Worker(w));
      partition.add(new ArrayList<>());
    }
    
    if (workerCount > 0) {
      for (J job : jobs) {
        Worker w = loads.poll();
        w.addWeight(weightFunction.apply(job));
        loads.add(w);
        partition.get(w.id).add(job);
      }
    }
    
    return partition;
  }
  
  private static class Worker implements Comparable<Worker>{
    private final int id;
    private double totalJobWeight;
    
    public Worker(int id) {
      this.id = id;
    }

    public void addWeight(double w) {
      totalJobWeight += w;
    }
    
    @Override
    public int compareTo(Worker o) {
      return Double.compare(totalJobWeight, o.totalJobWeight);
    }
  }
}
