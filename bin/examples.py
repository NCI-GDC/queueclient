from queueclient import RabbitMQClient

if __name__ == '__main__':
    qc = RabbitMQClient(host="172.21.23.222", username="dev_gdc", password="s3cr3t", queue_id="test_dev")
    qc.enqueue(msg=dict(did="AAAAA", size=12311))