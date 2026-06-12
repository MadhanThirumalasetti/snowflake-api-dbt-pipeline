import json
import boto3
import snowflake.connector
import logging
from datetime import datetime
from typing import List

logger = logging.getLogger(__name__)


class SnowflakeStageLoader:
    """
    Loads API records to S3 and triggers Snowpipe ingestion
    into Snowflake external stages.
    """

    def __init__(self, s3_bucket: str, s3_prefix: str, snowflake_conn: dict):
        self.s3_bucket = s3_bucket
        self.s3_prefix = s3_prefix
        self.s3_client = boto3.client("s3")
        self.snowflake_conn = snowflake_conn

    def _upload_to_s3(self, records: List[dict], entity: str) -> str:
        """Write records as newline-delimited JSON to S3."""
        timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        key = f"{self.s3_prefix}/{entity}/{timestamp}.json"
        body = "\n".join(json.dumps(r) for r in records)
        self.s3_client.put_object(Bucket=self.s3_bucket, Key=key, Body=body)
        logger.info(f"Uploaded {len(records)} records to s3://{self.s3_bucket}/{key}")
        return key

    def _copy_into_snowflake(self, stage_path: str, target_table: str):
        """Trigger COPY INTO from Snowflake external stage."""
        conn = snowflake.connector.connect(**self.snowflake_conn)
        try:
            cursor = conn.cursor()
            sql = f"""
                COPY INTO raw.{target_table}
                FROM @raw.s3_stage/{stage_path}
                FILE_FORMAT = (TYPE = 'JSON' STRIP_OUTER_ARRAY = FALSE)
                ON_ERROR = 'CONTINUE'
                PURGE = FALSE
            """
            cursor.execute(sql)
            result = cursor.fetchone()
            logger.info(f"COPY INTO result: {result}")
        finally:
            conn.close()

    def load(self, records: List[dict], entity: str, target_table: str):
        """Full load: S3 upload → Snowflake COPY INTO."""
        if not records:
            logger.info(f"No records to load for {entity}")
            return
        s3_key = self._upload_to_s3(records, entity)
        self._copy_into_snowflake(s3_key, target_table)
        logger.info(f"Successfully loaded {len(records)} {entity} records into {target_table}")
