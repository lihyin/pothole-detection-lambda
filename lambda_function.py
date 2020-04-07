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

CREATE TABLE training_data (
     uuid VARCHAR(40) NOT NULL,
     timestamp TIMESTAMP,
     longitude FLOAT,
     latitude FLOAT,
     device_id VARCHAR(40),
     image_filepath VARCHAR(256),
     is_new BOOLEAN DEFAULT true,
     potholes_json VARCHAR(5120),

     PRIMARY KEY (uuid)
)
'''

def mysql_save_insert(data):
    connection = pymysql.connect(host=mysql_config['host'],
                                 user=mysql_config['user'],
                                 password=mysql_config['password'],
                                 db=mysql_config['db'])
    uuid = None if 'uuid' not in data else data['uuid']
    longitude = None if 'longitude' not in data else float(data['longitude'])
    latitude = None if 'latitude' not in data else float(data['latitude'])
    device_id = None if 'device_id' not in data else data['device_id']
    image_filepath = None if 'image_filepath' not in data else data['image_filepath']
    for_training = None if 'for_training' not in data else bool(data['for_training'])
    potholes = None if 'potholes' not in data else data['potholes']

    result_message = ''
    i = 0
    for item in potholes:
        try:
            with connection.cursor() as cursor:
                boundingbox_x = None if 'boundingbox_x' not in item else int(item['boundingbox_x'])
                boundingbox_y = None if 'boundingbox_y' not in item else int(item['boundingbox_y'])
                boundingbox_width = None if 'boundingbox_width' not in item else int(item['boundingbox_width'])
                boundingbox_height = None if 'boundingbox_height' not in item else int(item['boundingbox_height'])
                confidence = None if 'confidence' not in item else float(item['confidence'])

                sql = "INSERT INTO pothole (uuid, boundingbox_x, boundingbox_y, boundingbox_width, boundingbox_height, confidence, timestamp, longitude, latitude, device_id, image_filepath) VALUES (%s, %s, %s, %s, %s, %s, now(), %s, %s, %s, %s)"

                val = (
                uuid + str(i), boundingbox_x, boundingbox_y, boundingbox_width, boundingbox_height, confidence, longitude, latitude,
                device_id, image_filepath)

                cursor.execute(sql, val)
                connection.commit()

                i += 1
                result_message += "Success to insert value: %s\n\r" % (str(val))
        except Exception as e:
            result_message += "Failed to insert, error: %s\n\r" % (e)

    # insert training data if for_training
    if for_training:
        try:
            with connection.cursor() as cursor:
                sql = "INSERT INTO training_data (uuid, timestamp, longitude, latitude, device_id, image_filepath, potholes_json) VALUES (%s, now(), %s, %s, %s, %s, %s)"

                if image_filepath is None:
                    image_filepath = uuid + ".jpg"
                val = (uuid, longitude, latitude, device_id, image_filepath, json.dumps(potholes))

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

data = json.loads('{"uuid":"af9a9f43-0e0b-4b4d-98ea-432b716b5c7d","potholes":[{"boundingbox_x":265,"boundingbox_y":167,"boundingbox_width":14,"boundingbox_height":6,"confidence":0.22750388}],"latitude":53.4200367,"longitude":-113.5198149,"device_id":"67fe6831e58a4310","for_training":true}')
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
