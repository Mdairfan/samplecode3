# samplecode3#importing os module in python for interacting with the operating system.
import os
#importing python client for hashicrop vault
import hvac
#importing package for base64 encoding and decoding
import base64
#boto3 python package for aws sdk
import boto3
from bson import ObjectId
import time
#apache spark python api
from pyspark import SparkContext
from pyspark.sql.functions import col
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType,StringType
from pyspark.sql.functions import udf
#AWS Encryption SDK implementation for Python
from dynamodb_encryption_sdk.encrypted.resource import EncryptedResource
from dynamodb_encryption_sdk.identifiers import CryptoAction
from dynamodb_encryption_sdk.encrypted.table import EncryptedTable
from dynamodb_encryption_sdk.material_providers.aws_kms import AwsKmsCryptographicMaterialsProvider
from dynamodb_encryption_sdk.structures import AttributeActions
#defination for text to get encrypted using hashicrop vault
def get_vault_encryption(text):
    client= hvac.Client(url="http://127.0.0.1:4200",token="xyz")
    encrypt_data_response = client.secrets.transit.encrypt_data(name = 'my-key',plaintext = str(base64.b64encode(str(text).encode("utf-8")), "utf-8"))
    return str(encrypt_data_response['data']['ciphertext'])


if __name__== "__main__":
    #connection with dynamodb table named vault_encrypt
    table = boto3.resource('dynamodb', region_name='us-east-1', verify=False).Table('vault_encrypt')
    aws_cmk_id ='arn:aws:kms:us-east-1:xxxxxxxxx:key/xxxxyyyyzzz'
    aws_kms_cmp = AwsKmsCryptographicMaterialsProvider(key_id=aws_cmk_id)
    actions = AttributeActions(default_action=CryptoAction.ENCRYPT_AND_SIGN,attribute_actions={'test': CryptoAction.DO_NOTHING})
    encrypted_table = EncryptedTable(table=table,materials_provider=aws_kms_cmp,attribute_actions=actions)
    #object of sparksession with app name as "Emr_Testing" 
    spark = SparkSession.builder.appName("EMR_Testing").getOrCreate()
    #creating sparkContext entry point for spark functionality
    sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    #reading paraquet file data stored in S3 bucket
    df = spark.read.parquet('s3n://tom/xyz.parquet')
    #applying user defined function of encrypting column named "birth_date"
    name_date = 'date'
    udf_encrypt_Function = udf(get_vault_encryption, StringType())
    spark.udf.register("udf_encrypt_Function", udf_encrypt_Function)
    new_df_date= df.select(*[udf_encrypt_Function(column).alias(name_date) if column == name_date else column for column in df.columns])
    #applying user defined function of encrypting column named "ss_id"
    name_ss_id='ss_id'
    new_df_ssn= new_df_date.select(*[udf_encrypt_Function(column).alias(name_ss_id) if column == name_ss_id else column for column in new_df_date.columns])
    specific_df=new_df_ssn.select([col(col_name).cast("string") for col_name in new_df_ssn.columns])
    #converting pyspark dataframe to dict
    new_rdd = specific_df.rdd.map(lambda row: row.asDict(True))
    for item in new_rdd.collect():# iterating over items in dict
        new_dict = {key: 'None' if value is None else value for (key, value) in item.items()}
        encrypted_table.put_item(Item=new_dict)#inserting item inside dynamodb table
    end1=time.time()

