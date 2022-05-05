import boto3, pprint, datetime, botocore
from botocore.errorfactory import ClientError

dbInstanceIdentifier = '<DB_INSTANCE_IDENTIFDIER_HERE>'
s3BucketIdentifier = '<S3_BUCKET_NAME_HERE>'
s3BucketRegion = 'ap-southeast-1'
filePrefix = "<FILE_PREFIX_IF_ANY>"

def lambda_handler(event, context):
    pp = pprint.PrettyPrinter(indent=4)
    rds = boto3.client('rds')
    s3 = boto3.resource('s3')
    bucket = boto3.resource('s3', region_name=s3BucketRegion).Bucket(s3BucketIdentifier)
    
    #we dont download the current log file as it may still change
    rdsResponse = rds.describe_db_log_files(
        DBInstanceIdentifier=dbInstanceIdentifier, 
        FilenameContains='audit.log.'
    )
    #pp.pprint(rdsResponse.get("DescribeDBLogFiles"))
    #Get the list of audit log files
    for idx, val in enumerate(rdsResponse.get("DescribeDBLogFiles")):
        auditLogDate = datetime.datetime.fromtimestamp(val["LastWritten"]/1000 + 28800)
        additionalDataPending = True;
        filePath = "/tmp/"
        fileName = filePrefix + auditLogDate.strftime('%Y%m%d_%H%M%S') + ".log"
        filePathName = filePath + fileName
        print(auditLogDate.tzinfo)
        remotePath = auditLogDate.strftime('%Y/%m') + "/"
        
        marker = "0"
        #pp.pprint(val)
        print ("Processing|" + str(val["LastWritten"]) + "|" + str(val["Size"]) + "|" + str(val["LogFileName"]) + "|" + fileName)
        #Find if the audit log file already exists, if it does we can skip and end processing because files are listed from latest first
        try:
            bucket.Object(remotePath + fileName).get()
            print ("Log file found, skipping...")
            break;
        except botocore.exceptions.ClientError as ex:
            if ex.response['Error']['Code'] == 'NoSuchKey':
                f = open(filePathName,"w")
                
                while additionalDataPending:
                    auditLogResponse = rds.download_db_log_file_portion(
                        DBInstanceIdentifier=dbInstanceIdentifier,
                        LogFileName=val["LogFileName"],
                        Marker=marker
                    )
                    f.write(auditLogResponse["LogFileData"])
                    print("Additional Data Pending = " + str(auditLogResponse["AdditionalDataPending"]) + " Marker = " + auditLogResponse["Marker"])
                    if auditLogResponse["AdditionalDataPending"]:
                        marker = auditLogResponse["Marker"]
                    additionalDataPending = auditLogResponse["AdditionalDataPending"]
                
                #pp.pprint(auditLogResponse)
                f.close()
                s3.meta.client.upload_file(filePathName, s3BucketIdentifier, remotePath + fileName)
                print ("Uploaded " + fileName + " to " + s3BucketIdentifier + "/" + remotePath)
    print ('Completed Processing')
    return 'Completed Processing'
