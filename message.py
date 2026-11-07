import json
import logging
from dataclasses import dataclass, asdict
from datetime import datetime, timezone

logger = logging.getLogger(__name__)


@dataclass
class Message:
    message_id: str
    producer_id: str
    text: str
    created_at: str

    def serialize(self) -> bytes:
        try:
            return json.dumps(
                asdict(self),
                ensure_ascii=False
            ).encode("utf-8")
        except (TypeError, ValueError) as exc:
            logger.exception("Serialization error: %s", exc)
            raise

    @classmethod
    def deserialize(cls, data: bytes) -> "Message":
        try:
            payload = json.loads(data.decode("utf-8"))

            return cls(
                message_id=payload["message_id"],
                producer_id=payload["producer_id"],
                text=payload["text"],
                created_at=payload["created_at"],
            )
        except (UnicodeDecodeError, ValueError, KeyError, TypeError) as exc:
            logger.exception("Deserialization error: %s", exc)
            raise

    @classmethod
    def create(cls, message_id: str, producer_id: str, text: str):
        return cls(
            message_id=message_id,
            producer_id=producer_id,
            text=text,
            created_at=datetime.now(timezone.utc).isoformat(),
        )
