## ChangeLog

---
## 1.0.0 - 2023-09-18
- Wrapper for EventBridgeScheduler for scheduling tasks
- In order to achieve high concurrency, for different type of schedules application can use different sqs queue.
  - For example for orders related schedule order queue and for order item related schedules order item queue.
- Application can use one queue for background worker in order to delegate task from main service to background
  - For example background-queue which will be subscribed by background service.
  - In this queue scheduler client in main service  will publish task.
  - Event bridge Scheduler will invoke background-queue target.
  - Background service will read task from this background queue and process.
- Application can use separate queue for processing task in main service
  - In this case scheduler client in main service will publish task to this queue.
  - Main service will subscribe this queue for task.
  - Event bridge Scheduler will invoke this target which will be read by main service subscriber.
- Wrapper over SQS, S3 and lambda
  

