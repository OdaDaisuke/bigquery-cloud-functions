# bigquery-cloud-functions
Operating bigquery from CloudFunctions(GCP)

## Usage

1. Create your GCP project.

2. Create Cloudfunctions project. Then create sample function and set this script.

3. Create BigQuery project.

4. Create BigQuery schema based on [these schema](https://github.com/OdaDaisuke/bigquery-cloud-functions/blob/master/index.js#L64-L151)

5. POST Request to the your cloud functions endpoint.

6. The request parameters will insert into your BigQuery tables.
