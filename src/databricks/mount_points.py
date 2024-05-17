{
 "cells": [
  {
   "cell_type": "code",
   "execution_count": 0,
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {
      "byteLimit": 2048000,
      "rowLimit": 10000
     },
     "inputWidgets": {},
     "nuid": "a9f1169e-285b-4d59-9bb2-4807d097ed94",
     "showTitle": false,
     "title": ""
    }
   },
   "outputs": [
    {
     "output_type": "stream",
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "Out[2]: True"
     ]
    }
   ],
   "source": [
    "\n",
    "dbutils.fs.mount(\n",
    "    source= 'wasbs://democontainer@storageaccount1430.blob.core.windows.net',\n",
    "    mount_point = '/mnt/blobpratibha',\n",
    "    extra_configs = {'fs.azure.account.key.storageaccount1430.blob.core.windows.net': 'Mwf1z4oPfpowxQrfmnZ1qa0PLe2wfuADSqDo5UvV2KpQz8r+q/hxFHq0Ufo0GfDlz2H+/KttW7wr+AStaunkdg=='}\n",
    ")"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 0,
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {
      "byteLimit": 2048000,
      "rowLimit": 10000
     },
     "inputWidgets": {},
     "nuid": "f0510b46-a02f-4d3b-ba1a-aff8bcc44346",
     "showTitle": false,
     "title": ""
    }
   },
   "outputs": [
    {
     "output_type": "display_data",
     "data": {
      "text/html": [
       "<style scoped>\n",
       "  .table-result-container {\n",
       "    max-height: 300px;\n",
       "    overflow: auto;\n",
       "  }\n",
       "  table, th, td {\n",
       "    border: 1px solid black;\n",
       "    border-collapse: collapse;\n",
       "  }\n",
       "  th, td {\n",
       "    padding: 5px;\n",
       "  }\n",
       "  th {\n",
       "    text-align: left;\n",
       "  }\n",
       "</style><div class='table-result-container'><table class='table-result'><thead style='background-color: white'><tr><th>department_id</th><th>department_name</th></tr></thead><tbody><tr><td>D107</td><td>Engineering</td></tr><tr><td>D107</td><td>Engineering</td></tr><tr><td>D108</td><td>Medical</td></tr><tr><td>D108</td><td>Medical</td></tr></tbody></table></div>"
      ]
     },
     "metadata": {
      "application/vnd.databricks.v1+output": {
       "addedWidgets": {},
       "aggData": [],
       "aggError": "",
       "aggOverflow": false,
       "aggSchema": [],
       "aggSeriesLimitReached": false,
       "aggType": "",
       "arguments": {},
       "columnCustomDisplayInfos": {},
       "data": [
        [
         "D107",
         "Engineering"
        ],
        [
         "D107",
         "Engineering"
        ],
        [
         "D108",
         "Medical"
        ],
        [
         "D108",
         "Medical"
        ]
       ],
       "datasetInfos": [],
       "dbfsResultPath": null,
       "isJsonSchema": true,
       "metadata": {},
       "overflow": false,
       "plotOptions": {
        "customPlotOptions": {},
        "displayType": "table",
        "pivotAggregation": null,
        "pivotColumns": null,
        "xColumns": null,
        "yColumns": null
       },
       "removedWidgets": [],
       "schema": [
        {
         "metadata": "{}",
         "name": "department_id",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "department_name",
         "type": "\"string\""
        }
       ],
       "type": "table"
      }
     },
     "output_type": "display_data"
    }
   ],
   "source": [
    "df = spark.read.csv(\"dbfs:/mnt/blobpratibha/Department-Q1.csv\", header=True, inferSchema=True)\n",
    "df.display()"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 0,
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {
      "byteLimit": 2048000,
      "rowLimit": 10000
     },
     "inputWidgets": {},
     "nuid": "75a57813-ffff-4564-8ac5-7283c702e686",
     "showTitle": false,
     "title": ""
    }
   },
   "outputs": [],
   "source": [
    "from pyspark.sql.types import StructType, StructField, StringType\n",
    "\n",
    "custom_schema = StructType([\n",
    "    StructField(\"department_id\", StringType(), True),\n",
    "    StructField(\"department_name\", StringType(), True)\n",
    "])\n",
    "\n",
    "df = spark.read.csv(\"dbfs:/mnt/blobpratibha/Department-Q1.csv\", header=True, schema=custom_schema)\n"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 0,
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {
      "byteLimit": 2048000,
      "rowLimit": 10000
     },
     "inputWidgets": {},
     "nuid": "786f9a29-6ee4-4c28-89b8-6e5a92954a4b",
     "showTitle": false,
     "title": ""
    }
   },
   "outputs": [],
   "source": [
    "custom_schema = StructType([\n",
    "    StructField(\"department_id\", StringType(), True),\n",
    "    StructField(\"department_name\", StringType(), True)\n",
    "])\n",
    "\n",
    "\n",
    "data = [(\"D107\", \"Engineering\"),\n",
    "        (\"D108\", \"Medical\"),\n",
    "       ]\n",
    "df = spark.createDataFrame(data, schema=custom_schema)\n",
    "\n",
    "df.write.csv(\"dbfs:/mnt/blobpratibha/Department-Q1.csv\", header=True, mode='overwrite')\n",
    "\n"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 0,
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {
      "byteLimit": 2048000,
      "rowLimit": 10000
     },
     "inputWidgets": {},
     "nuid": "9940b506-34fb-4b30-87f8-4c1769b6ba51",
     "showTitle": false,
     "title": ""
    }
   },
   "outputs": [],
   "source": [
    "df = spark.read.csv(\"dbfs:/mnt/blobpratibha/Department-Q1.csv\", header=True, schema=custom_schema)\n"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 0,
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {
      "byteLimit": 2048000,
      "rowLimit": 10000
     },
     "inputWidgets": {},
     "nuid": "55c219cb-93df-416a-9f92-b6b71a4cb545",
     "showTitle": false,
     "title": ""
    }
   },
   "outputs": [],
   "source": [
    "\n",
    "custom_schema = StructType([\n",
    "    StructField(\"address\", StringType(), True),\n",
    "    StructField(\"email\", StringType(), True),\n",
    "    StructField(\"hexcolor\", StringType(), True),\n",
    "    StructField(\"name\", StringType(), True),\n",
    "    StructField(\"phoneNumber\", StringType(), True),\n",
    "    StructField(\"userAgent\", StringType(), True)\n",
    "])\n",
    "\n",
    "json_file_path = \"dbfs:/FileStore/sample_2_for_practice.json\"\n",
    "\n",
    "df_json = spark.read.schema(custom_schema).json(json_file_path, multiLine=True)\n",
    "\n",
    "\n"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 0,
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {
      "byteLimit": 2048000,
      "rowLimit": 10000
     },
     "inputWidgets": {},
     "nuid": "91716cca-8984-4a9c-86c2-ab06fbe4cc3c",
     "showTitle": false,
     "title": ""
    }
   },
   "outputs": [
    {
     "output_type": "display_data",
     "data": {
      "text/plain": [
       ""
      ]
     },
     "metadata": {
      "application/vnd.databricks.v1+output": {
       "arguments": {},
       "data": "",
       "errorSummary": "Command skipped",
       "errorTraceType": "ansi",
       "metadata": {},
       "type": "ipynbError"
      }
     },
     "output_type": "display_data"
    }
   ],
   "source": [
    "\n",
    "\n",
    "df_parquet = spark.read.parquet(\"dbfs:/dbfs/tmp/parquet_file.parquet\")\n"
   ]
  }
 ],
 "metadata": {
  "application/vnd.databricks.v1+notebook": {
   "dashboards": [],
   "environmentMetadata": null,
   "language": "python",
   "notebookMetadata": {
    "pythonIndentUnit": 4
   },
   "notebookName": "mount_points",
   "widgets": {}
  }
 },
 "nbformat": 4,
 "nbformat_minor": 0
}
