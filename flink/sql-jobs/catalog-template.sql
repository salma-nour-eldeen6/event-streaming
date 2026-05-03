-- Catalog Template SQL
DROP CATALOG IF EXISTS iceberg;
CREATE CATALOG iceberg WITH (
    'type' = 'iceberg',
    'catalog-impl' = 'org.apache.iceberg.rest.RESTCatalog',
    'uri' = '${ICEBERG_REST_URI}',
    'warehouse' = '${ICEBERG_WAREHOUSE}',
    'io-impl' = 'org.apache.iceberg.aws.s3.S3FileIO',
    's3.endpoint' = '${MINIO_ENDPOINT}',
    's3.path-style-access' = 'true',
    'client.region' = '${AWS_REGION}',
    's3.access-key-id' = '${MINIO_ACCESS_KEY}',
    's3.secret-access-key' = '${MINIO_SECRET_KEY}'
);