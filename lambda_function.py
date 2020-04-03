import json
import pymysql
import os

print('Loading function')

mysql_config = {
    'host': os.environ['db_host'],
    'user': os.environ['db_user'],
    'password': os.environ['db_password'],
    'db': os.environ['db']
}

'''
CREATE TABLE pothole (
     uuid VARCHAR(40) NOT NULL,
     boundingbox_x INT,
     boundingbox_y INT,
     boundingbox_width INT,
     boundingbox_height INT,
     confidence FLOAT,
     timestamp TIMESTAMP,
     longitude FLOAT,
     latitude FLOAT,
     device_id VARCHAR(40),
     image_filepath VARCHAR(256),

     PRIMARY KEY (uuid)
)
'''

def mysql_save_insert(data):
    connection = pymysql.connect(host=mysql_config['host'],
                                 user=mysql_config['user'],
                                 password=mysql_config['password'],
                                 db=mysql_config['db'])
    result_message = ''
    for item in data:
        try:
            with connection.cursor() as cursor:
                uuid = None if 'uuid' not in item else item['uuid']
                boundingbox_x = None if 'boundingbox_x' not in item else int(item['boundingbox_x'])
                boundingbox_y = None if 'boundingbox_y' not in item else int(item['boundingbox_y'])
                boundingbox_width = None if 'boundingbox_width' not in item else int(item['boundingbox_width'])
                boundingbox_height = None if 'boundingbox_height' not in item else int(item['boundingbox_height'])
                confidence = None if 'confidence' not in item else float(item['confidence'])
                longitude = None if 'longitude' not in item else float(item['longitude'])
                latitude = None if 'latitude' not in item else float(item['latitude'])
                device_id = None if 'device_id' not in item else item['device_id']
                image_filepath = None if 'image_filepath' not in item else item['image_filepath']

                sql = "INSERT INTO pothole (uuid, boundingbox_x, boundingbox_y, boundingbox_width, boundingbox_height, confidence, timestamp, longitude, latitude, device_id, image_filepath) VALUES (%s, %s, %s, %s, %s, %s, now(), %s, %s, %s, %s)"

                val = (
                uuid, boundingbox_x, boundingbox_y, boundingbox_width, boundingbox_height, confidence, longitude, latitude,
                device_id, image_filepath)

                print(val)

                cursor.execute(sql, val)
                connection.commit()

                result_message += "Success to insert value: %s\n\r" % (str(val))
        except Exception as e:
            result_message += "Failed to insert, error: %s\n\r" % (e)

    print(result_message)
    connection.close()
    return result_message

def payload_handler(data):
    # insert the data into mysql database
    message = mysql_save_insert(data)

    return {'message': message}

data = json.loads("[{\"uuid\":\"f2c1e7fe-e9e1-4adf-bdcc-a1d957253fc2-0\",\"boundingbox_x\":250,\"boundingbox_y\":214,\"boundingbox_width\":26,\"boundingbox_height\":9,\"confidence\":0.17546588},{\"uuid\":\"f2c1e7fe-e9e1-4adf-bdcc-a1d957253fc2-1\",\"boundingbox_x\":289,\"boundingbox_y\":239,\"boundingbox_width\":32,\"boundingbox_height\":10,\"confidence\":0.10995752}]")
payload_handler(data)

def respond(err, res=None):
    return {
        'statusCode': '400' if err else '200',
        'body': err.message if err else json.dumps(res),
        'headers': {
            'Content-Type': 'application/json',
        },
    }


def lambda_handler(event, context):
    '''Demonstrates a simple HTTP endpoint using API Gateway. You have full
    access to the request and response payload, including headers and
    status code.

    To scan a DynamoDB table, make a GET request with the TableName as a
    query string parameter. To put, update, or delete an item, make a POST,
    PUT, or DELETE request respectively, passing in the payload to the
    DynamoDB API as a JSON body.
    '''
    operations = {
        'POST': lambda data
        : payload_handler(data),
    }

    operation = event['httpMethod']
    if operation in operations:
        print(event['body'])
        data = json.loads(event['body'])
        print(data)

        response = respond(None, operations[operation](data))
        print(response)
        return response
    else:
        response = respond(ValueError('Unsupported method "{}"'.format(operation)))
        print(response)
        return response
