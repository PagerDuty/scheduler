## Useful graphs on a Datadog dashboard

The library, if so configured, sends metrics on all its essential operations. In the [operations guide](operations.md),
we mention a couple of them. This document contains details on the graphs on our Scheduler dashboard. Graph definitions
contain two variables:

- `$service` - which application is running
- `$scope` - in which environment (prod, staging) it is running.

These variables can be setup when configuring the `Metrics` instance for Scheduler.

### Tasks Enqueued to Kafka (success count)

    {
      "viz": "timeseries",
      "requests": [
        {
          "q": "sum:pd_scheduler.task_enqueue_to_kafka.count{$scope,result:success,$service} by {host}.as_count()",
          "aggregator": "avg",
          "conditional_formats": [],
          "type": "bars"
        }
      ],
      "events": [
        {
          "q": "tags:$scope Rebalancing Kafka Consumers $service"
        }
      ]
    }
    
### Tasks Enqueued to Kafka (failure count)
  
    {
      "viz": "timeseries",
      "requests": [
        {
          "q": "sum:pd_scheduler.task_enqueue_to_kafka.count{$scope,result:failure,$service} by {exception}.as_count()",
          "aggregator": "avg",
          "style": {
            "palette": "warm"
          },
          "type": "bars",
          "conditional_formats": []
        }
      ],
      "events": [
        {
          "q": "tags:$scope Rebalancing Kafka Consumers $service"
        }
      ]
    }
    
### Tasks Enqueued to Kafka (time in ms)

    {
      "viz": "timeseries",
      "requests": [
        {
          "q": "avg:pd_scheduler.task_enqueue_to_kafka.95percentile{$scope,result:success,$service} by {host}",
          "aggregator": "avg",
          "conditional_formats": [],
          "type": "line"
        }
      ],
      "events": [
        {
          "q": "tags:$scope Rebalancing Kafka Consumers $service"
        }
      ],
      "markers": [
        {
          "max": 200,
          "min": 0,
          "type": "ok dashed",
          "value": "0 < y < 200",
          "dim": "y"
        },
        {
          "max": 400,
          "min": 200,
          "type": "warning dashed",
          "value": "200 < y < 400",
          "dim": "y"
        },
        {
          "max": null,
          "min": 400,
          "type": "error dashed",
          "value": "y > 400",
          "dim": "y"
        }
      ]
    }
    
### Persist Polled Task Batch to Cass (persisted task count)

    {
      "viz": "timeseries",
      "requests": [
        {
          "q": "sum:pd_scheduler.tasks_persist_to_cass{$scope,$service} by {partition}.as_count()",
          "aggregator": "avg",
          "conditional_formats": [],
          "type": "bars",
          "style": {
            "palette": "cool"
          }
        }
      ],
      "events": [
        {
          "q": "tags:$scope Rebalancing Kafka Consumers $service"
        }
      ]
    }
    
    
### Persist Polled Task Batch to Cass (failure count)
 
    {
      "viz": "timeseries",
      "requests": [
        {
          "q": "sum:pd_scheduler.tasks_sent_to_akka.count{$scope,result:failure,$service} by {exception}.as_count()",
          "aggregator": "avg",
          "conditional_formats": [],
          "type": "bars",
          "style": {
            "palette": "warm"
          }
        }
      ],
      "events": [
        {
          "q": "tags:$scope Rebalancing Kafka Consumers $service"
        }
      ]
    }
   
### Persist Polled Task Batch to Cass (time taken in ms)

    {
      "viz": "timeseries",
      "requests": [
        {
          "q": "avg:pd_scheduler.tasks_sent_to_akka.95percentile{$scope,result:success,$service} by {host}",
          "aggregator": "avg",
          "conditional_formats": [],
          "type": "line"
        }
      ],
      "events": [
        {
          "q": "tags:$scope Rebalancing Kafka Consumers $service"
        }
      ],
      "markers": [
        {
          "max": 100,
          "min": 0,
          "type": "ok dashed",
          "value": "0 < y < 100",
          "dim": "y"
        },
        {
          "max": 200,
          "min": 100,
          "type": "warning dashed",
          "value": "100 < y < 200",
          "dim": "y"
        },
        {
          "max": null,
          "min": 200,
          "type": "error dashed",
          "value": "y > 200",
          "dim": "y"
        }
      ]
    }

### Task Execution (success count)

    {
      "viz": "timeseries",
      "requests": [
        {
          "q": "sum:pd_scheduler.task_execution.count{$scope,result:success,$service} by {type}.as_count()",
          "aggregator": "avg",
          "conditional_formats": [],
          "type": "bars"
        }
      ],
      "events": [
        {
          "q": "tags:$scope Rebalancing Kafka Consumers $service"
        }
      ]
    }
    
### Task Execution (failure count)

    {
      "viz": "timeseries",
      "requests": [
        {
          "q": "sum:pd_scheduler.task_execution.count{$scope,result:failure,$service} by {exception}.as_count()",
          "aggregator": "avg",
          "conditional_formats": [],
          "type": "bars",
          "style": {
            "palette": "warm"
          }
        }
      ],
      "events": [
        {
          "q": "tags:$scope Rebalancing Kafka Consumers $service"
        }
      ]
    }


### Task Execution (duration in ms)

    {
      "viz": "timeseries",
      "requests": [
        {
          "q": "max:pd_scheduler.task_execution_duration.max{$scope,$service} by {type}",
          "aggregator": "avg",
          "conditional_formats": [],
          "type": "line"
        }
      ],
      "events": [
        {
          "q": "tags:$scope Rebalancing Kafka Consumers $service"
        }
      ]
    }
    
### Tasks in Memory (count)

    {
      "viz": "timeseries",
      "requests": [
        {
          "q": "sum:pd_scheduler.partitionScheduler_task_count.max{$scope,$service}, sum:pd_scheduler.partitionExecutor_task_count.max{$scope,$service}",
          "aggregator": "avg",
          "conditional_formats": [],
          "type": "area"
        }
      ],
      "events": [
        {
          "q": "tags:$scope Rebalancing Kafka Consumers $service"
        }
      ]
    }
    
### Stale Tasks (older than 5 minutes count)

    {
      "viz": "timeseries",
      "requests": [
        {
          "q": "max:pd_scheduler.stale_task_count.max{$scope,$service} by {host}",
          "aggregator": "avg",
          "style": {
            "palette": "warm"
          },
          "type": "line",
          "conditional_formats": []
        }
      ],
      "events": [
        {
          "q": "tags:$scope Rebalancing Kafka Consumers $service"
        }
      ]
    }
    
### Task Execution (delay from target time in min)

    {
      "viz": "timeseries",
      "requests": [
        {
          "q": "max:pd_scheduler.task_execution_delay.max{$scope,$service} by {type} / 60000",
          "aggregator": "avg",
          "conditional_formats": [],
          "type": "line"
        }
      ],
      "events": [
        {
          "q": "tags:$scope Rebalancing Kafka Consumers $service"
        }
      ]
    }
    
### System Level Errors (count)

    {
      "viz": "timeseries",
      "requests": [
        {
          "q": "sum:pd_scheduler.task_retries_exhausted{$scope,$service}.as_count()",
          "aggregator": "avg",
          "conditional_formats": [],
          "type": "bars",
          "style": {
            "palette": "grey"
          }
        },
        {
          "q": "sum:pd_scheduler.sys_restart_on_error{$scope,$service}.as_count()",
          "aggregator": "avg",
          "conditional_formats": [],
          "type": "bars",
          "style": {
            "palette": "warm"
          }
        }
      ]
    }
    

