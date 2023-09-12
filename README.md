### Common Utilities 

This library contains various utilities required by different projects.

### How to install
Open terminal and run below commands.
````
git clone git@github.com:tata1mg/commonutils.git
cd commonutils/

python3 setup.py install

commonutils can be installed via

pip3 install git+ssh://git@github.com/tata1mg/commonutils.git

````

### How to use 
Add this library as dependency in your project.
This project is made public for use in selected projects.
Soon we will make open source version of this.

##### Using SQS consumer to process messages in your Sanic service

###### Add task to your Sanic app
``` 
Example For SQS: create handler eventHandler and override handle_event menthod.
from commonutils.models import SQSHandler

class eventHandler(SQSHandler):
    """
    Handle SQS messages
    """
    @classmethod
    async def handle_event(cls, payload):
        try:
            logger.info("Processing payload", payload)
        except Exception as exception:
            raise Exception("Unable to process update from SQS", exception.msg)
 
async def initialize_sqs_subscriber(_app, loop):
    logger.info("Starting SQS consumer")
    baseSQSwrapper = BaseSQSWrapper(CONFIG.config)
    await baseSQSwrapper.get_sqs_client(queue_name='queue_name)
    _app.add_task(baseSQSwrapper.subscribe_all(eventHandler, max_no_of_messages = 5, wait_time_in_seconds= 5)) // in case 
    of processing messaged in background

above method may be set as listener in sanic app

Host._listeners = [
        (clear_function_cache, "after_server_start"),
        ...
        ...
        ...
        (initialize_sqs_subscriber, "after_server_start")
    ]
```

### How to raise issues
Please use github issues to raise any bug or feature request

### Discussions

Please use github discussions for any topic related to this project

### Contributions

Soon we will be inviting open source contributions.

### Supported python versions
3.7.x,3.8.x,3.9.x






