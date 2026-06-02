"""Build Kafka payloads from scenario definitions."""

import json
from datetime import datetime, timedelta, timezone

from kafka import KafkaProducer

from app.config import Config
from app.schemas import CartEventPayload
from app.scenarios import ScenarioEvent


def utc_timestamp_iso(offset_seconds: int = 0) -> str:
    moment = datetime.now(timezone.utc) + timedelta(seconds=offset_seconds)
    return moment.strftime('%Y-%m-%dT%H:%M:%S')


def build_payload(spec: ScenarioEvent, run_id: str) -> CartEventPayload:
    session_id = spec.session_id or f"sess_scenario_{spec.user_id}"
    event_id = f"{run_id}-{spec.event_id}" if run_id else spec.event_id
    payload = CartEventPayload(
        event_id=event_id,
        user_id=spec.user_id,
        event_type=spec.event_type,
        product_id=spec.product_id,
        product_name=spec.product_name,
        quantity=spec.quantity,
        price=spec.price,
        timestamp=utc_timestamp_iso(spec.timestamp_offset_seconds),
        session_id=session_id,
    )
    payload.validate()
    return payload


def create_producer(config: Config) -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=config.KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda value: json.dumps(value).encode('utf-8'),
        key_serializer=lambda key: key.encode('utf-8'),
        acks='all',
        retries=3,
    )


def publish(
    producer: KafkaProducer,
    config: Config,
    spec: ScenarioEvent,
    run_id: str,
) -> CartEventPayload:
    payload = build_payload(spec, run_id)
    body = payload.to_dict()
    producer.send(
        config.KAFKA_TOPIC,
        key=str(payload.user_id),
        value=body,
    )
    return payload
