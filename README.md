> [!NOTE]
> The code in this repository has been made public as-is for informational purposes. The repository may use private resources for the building and execution of the code. For example, private registries may be used for dependency resolution.
>
> The documentation may refer to restricted URLs.

[![pre-commit](https://img.shields.io/badge/pre--commit-enabled-brightgreen?logo=pre-commitlogoColor=white)](https://github.com/pre-commit/pre-commit)

---
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

    q = DepotQueueClient(host="depot.service.consul", queue_id="uuid")
    assert q.status() is True

    # msg can be any json object
    response = q.enqueue(msg=dict(did="AAAAA", size=123))
```

```python
    from queueclient import DepotQueueClient

    q = DepotQueueClient(host="depot.service.consul", queue_id="uuid")

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
    def consumer_callback(body):
        msg = json.loads(body)

        assert msg
        assert msg["did"] == "AAAAA"
        assert msg["size"] == 123

    q = RabbitMQClient(host="localhost", port=5672, vhost="/", queue_id="qid", username="guest", password="guest")
    response = q.enqueue(msg=dict(did="AAAAA", size=123))
    assert response is True

    # this line blocks, call q.close() to cancel
    q.consume(consumer_callback)
```


## Setup pre-commit hook to check for secrets

We use [pre-commit](https://pre-commit.com/) to setup pre-commit hooks for this repo.
We use [detect-secrets](https://github.com/Yelp/detect-secrets) to search for secrets being committed into the repo.

To install the pre-commit hook, run
```
pre-commit install
```

To update the .secrets.baseline file run
```
detect-secrets scan --update .secrets.baseline
```

`.secrets.baseline` contains all the string that were caught by detect-secrets but are not stored in plain text. Audit the baseline to view the secrets .

```
detect-secrets audit .secrets.baseline
```

### Running Test
Tests uses [pifpaf](https://pypi.org/project/pifpaf/) to spin up daemon rabbitmq-server locally which requires RabbitMQ installed, to install on ubuntu
```bash
$ sudo apt install rabbitmq-server
```

run tests via tox
```bash
$ tox
```

or manually
```bash
$ python -m pip install .
$ python -m pip install -r dev-requirements.txt
$ pifpaf run rabbitmq -- python -m pytest
```
