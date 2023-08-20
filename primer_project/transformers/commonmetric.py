import datetime
import logging
import os
from dataclasses import dataclass
from sqlite3 import Cursor


logging.basicConfig(level=logging.WARN, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)
logger.setLevel(os.environ.get("LOGLEVEL", "INFO"))


SQL_QUERY = """
    SELECT
        "event_v2_data".event_id,
        "event_v2_data".flow_id,
        "event_v2_data".created_at,
        "event_v2_data".transaction_lifecycle_event,
        json_extract("event_v2_data"."error_details", '$.decline_reason') AS decline_reason,
        json_extract("event_v2_data"."error_details", '$.decline_type') AS decline_type,
        json_extract("transaction_request"."vault_options", '$.payment_method') AS payment_method,
        "transaction".transaction_id,
        "transaction".transaction_type,
        "transaction".amount,
        "transaction".currency_code,
        "transaction".processor_merchant_account_id,
        "payment_instrument_token_data".three_d_secure_authentication,
        "payment_instrument_token_data".payment_instrument_type,
        json_extract("payment_instrument_token_data"."vault_data", '$.customer_id') AS customer_id
    FROM
        "event_v2_data"
    INNER JOIN "transaction"
        ON "event_v2_data"."transaction_id" = "transaction"."transaction_id"
    INNER JOIN "transaction_request"
        ON "event_v2_data"."flow_id" = "transaction_request"."flow_id"
    INNER JOIN "payment_instrument_token_data"
        ON "payment_instrument_token_data"."token_id" = "transaction_request"."token_id"
"""


@dataclass
class CommonMetric():
    """
    Schema of a final data to be inserted into the output database.
    """
    event_id: str
    flow_id: str
    created_at: datetime.datetime
    transaction_lifecycle_event: str
    decline_reason: str
    decline_type: str
    payment_method: str
    transaction_id: str
    transaction_type: str
    amount: float
    currency_code: str
    processor_merchant_account_id: str
    three_d_secure_authentication: str
    payment_instrument_type: str
    customer_id: str


def extract_common_metrics(cursor: Cursor):
    """
    Extracts metrics from the database.
    """
    return [
        CommonMetric(**{
            k: v
            for k, v
            in zip([c[0] for c in cursor.description], row)
        })
        for row in cursor.execute(SQL_QUERY).fetchall()
    ]
