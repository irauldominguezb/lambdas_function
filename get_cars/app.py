import json
import pymysql
from decimal import Decimal

rds_host = "carsdb.ct6a08gawxrz.us-east-1.rds.amazonaws.com"
rds_user = "admin"
rds_password = "121003rD"
rds_db = "carsd"


def decimal_to_float(obj):
    if isinstance(obj, Decimal):
        return float(obj)
    raise TypeError


def lambda_handler(event, __):
    try:
        result = get_all_cars()

        body = {
            "message": "CARS_FETCHED",
            "cars": result
        }
        return {
            "statusCode": 200,
            "body": json.dumps(body, default=decimal_to_float)
        }
    except Exception as e:
        return {
            "statusCode": 500,
            "body": json.dumps({
                "message": "INTERNAL_SERVER_ERROR",
                "error": str(e)
            }),
        }


def get_all_cars():
    connection = pymysql.connect(host=rds_host, user=rds_user, password=rds_password, db=rds_db)
    try:
        cursor = connection.cursor()
        cursor.execute("SELECT * FROM cars;")

        result = cursor.fetchall()
        result = [dict(zip([column[0] for column in cursor.description], row)) for row in result]

        return result
    except Exception as e:
        raise e
    finally:
        connection.close()
