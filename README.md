## Quick Guide

Simple queue client which provides a generic interface for interacting with multiple queuing systems, currently supports:
- Depot
- RabbitMQ
- A simple in memory queue

### In Memory Queues
implementation based on pythons double ended queue

Sample usage:

```python
    from queueclient import InMemoryQueueClient
    
    q = InMemoryQueueClient(queue_id='uuid')
    
    # msg can be any python object
    q.enqueue(msg="message")
    
    # from worker
    msg = q.dequeue()
    assert msg == "message"

```

### Depot

Sample usage:

```python
    from queueclient import DepotQueueClient
    
    q = DepotQueueClient("http://depot.service", queue_id="uuid")
    assert q.status() is True

    # msg can be any json object
    response = q.enqueue(msg=dict(did="AAAAA", size=123))
```

```python
    from queueclient import DepotQueueClient
    
    q = DepotQueueClient("http://depot.service", queue_id="uuid")

    # retrieve previous message
    msg = q.dequeue()
    assert msg
    assert msg["did"] == "AAAAA"
    assert msg["size"] == 123

```

### RabbitMQ

Sample usage:

```python
    import json
    from queueclient import RabbitMQClient
    
    # requires a custom call back function, and assumes body is a json object
    def consumer_callback(ch, mtd, props, body):
        msg = json.loads(body)

        assert msg
        assert msg["did"] == "AAAAA"
        assert msg["size"] == 123
        ch.basic_ack(delivery_tag=mtd.delivery_tag)
    
    q = RabbitMQClient(host="localhost", port=5672, vhost="/", queue_id="qid", username="guest", password="guest")
    response = q.enqueue(msg=json.dumps(dict(did="AAAAA", size=123)))
    assert response is True
    
    # this line blocks, call q.close() to cancel
    q.consume(consumer_callback)
```
