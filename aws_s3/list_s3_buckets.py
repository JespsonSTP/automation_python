import boto3

#get aws management console
aws_mng_con=boto3.session.Session(profile_name='default')

#get acces to s3 services
my_aws_s3=aws_mng_con.resource(service_name='s3')
my_aws_s3_client=aws_mng_con.client(service_name='s3')

for all_bucket in my_aws_s3.buckets.all():
   print(all_bucket.name)


#print(my_aws_s3_client.list_buckets()['Buckets'])

for my_buckets in my_aws_s3_client.list_buckets()['Buckets']:
    print(my_buckets['Name'])

