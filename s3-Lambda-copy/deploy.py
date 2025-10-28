import os
import zipfile
import boto3
import botocore
import argparse
import logging


logging.basicConfig(filename="logs.log",level=logging.INFO,format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger()

STACK_NAME = "s3-lambda-copy-stack"
REGION = "ap-south-1"


def zip_lambda(source_dir, zip_filename):
    with zipfile.ZipFile(zip_filename, "w", zipfile.ZIP_DEFLATED) as zf:
        for root, _, files in os.walk(source_dir):
            for f in files:
                full_path = os.path.join(root, f)
                arcname = os.path.relpath(full_path, source_dir)
                zf.write(full_path, arcname)
    logger.info("Created ZIP: %s", zip_filename)


def upload_artifact(bucket, key, file_path, s3):
    logger.info("Uploading %s to s3", file_path)
    s3.upload_file(file_path, bucket, key)
    logger.info("Uploaded successfully")


def deploy_stack(cfn, template_body, params):
    try:
        cfn.describe_stacks(StackName=STACK_NAME)
        exists = True
    except botocore.exceptions.ClientError:
        exists = False

    if exists:
        logger.info("Updating stack:")
        resp = cfn.update_stack(
            StackName=STACK_NAME,
            TemplateBody=template_body,
            Parameters=params,
            Capabilities=['CAPABILITY_NAMED_IAM']
        )
        waiter = cfn.get_waiter('stack_update_complete')
    else:
        logger.info("Creating stack:")
        resp = cfn.create_stack(
            StackName=STACK_NAME,
            TemplateBody=template_body,
            Parameters=params,
            Capabilities=['CAPABILITY_NAMED_IAM']
        )
        waiter = cfn.get_waiter('stack_create_complete')

    logger.info("Stack operation started: %s", resp['StackId'])
    waiter.wait(StackName=STACK_NAME)
    logger.info("Stack creation/update complete!")


def main_func():
    parser = argparse.ArgumentParser()
    parser.add_argument("--deployment-bucket")
    parser.add_argument("--deployment-key", default="lambda_artifact.zip")
    parser.add_argument("--source-bucket")
    parser.add_argument("--destination-bucket")
    parser.add_argument("--region", default=REGION)
    args = parser.parse_args()

    zip_path = "lambda_artifact.zip"
    zip_lambda("Lambda_Function", zip_path)

    s3 = boto3.client("s3", region_name=args.region)
    upload_artifact(args.deployment_bucket, args.deployment_key, zip_path, s3)

    with open("template.yaml") as f:
        template_body = f.read()

    params = [
        {"ParameterKey": "DeploymentBucketName", "ParameterValue": args.deployment_bucket},
        {"ParameterKey": "DeploymentObjectKey", "ParameterValue": args.deployment_key},
        {"ParameterKey": "SourceBucketName", "ParameterValue": args.source_bucket},
        {"ParameterKey": "DestinationBucketName", "ParameterValue": args.destination_bucket},
    ]

    cfn = boto3.client("cloudformation", region_name=args.region)
    deploy_stack(cfn, template_body, params)


if __name__ == "__main__":
    main_func()
