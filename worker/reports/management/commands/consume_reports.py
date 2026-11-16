import json
import os
import time
from django.core.management.base import BaseCommand
from kafka import KafkaConsumer, KafkaProducer
from reports.processing import make_result


class Command(BaseCommand):
    help = "Consume report requests and publish results in a separate Python process"

    def handle(self, *args, **options):
        broker = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
        consumer = KafkaConsumer(
            "report-requests", bootstrap_servers=broker,
            group_id="django-report-workers", auto_offset_reset="earliest",
            enable_auto_commit=False, max_poll_records=1,
            value_deserializer=lambda value: json.loads(value.decode("utf-8")),
        )
        producer = KafkaProducer(
            bootstrap_servers=broker, acks="all",
            key_serializer=lambda key: key.encode("utf-8"),
            value_serializer=lambda value: json.dumps(value).encode("utf-8"),
        )
        self.stdout.write("Django worker listening on report-requests")
        try:
            for message in consumer:
                event = message.value
                self.stdout.write(f"Received reportId={event['reportId']} offset={message.offset}")
                time.sleep(float(os.environ.get("PROCESSING_SECONDS", "3")))
                result = make_result(event)
                producer.send("report-results", key=result["reportId"], value=result).get(timeout=15)
                # Commit only AFTER Kafka has acknowledged the result.
                # A crash before commit can cause a duplicate: Spring handles that safely.
                consumer.commit()
                self.stdout.write(f"Published ReportCompleted reportId={result['reportId']}")
        finally:
            consumer.close(autocommit=False)
            producer.close(timeout=5)
