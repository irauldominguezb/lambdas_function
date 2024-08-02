import json
import pymysql
import logging

rds_host = "carsdb.ct6a08gawxrz.us-east-1.rds.amazonaws.com"
rds_user = "admin"
rds_password = "121003rD"
rds_db = "carsd"

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def lambda_handler(event, __):
    try:
        if 'body' not in event:
            logger.error("Request body not found in the event")
            raise KeyError('body')

        body = json.loads(event['body'])

        car = body

        required_keys = ['marca', 'modelo', 'autonomia_electrica', 'autonomia_combustible']
        for key in required_keys:
            if key not in car:
                logger.error(f" el campo {key} es requerido para guardar un carro")
                raise KeyError(key)

        save_car(car)

        return {
            "statusCode": 200,
            "body": json.dumps({
                "message": "Car saved successfully",
                "car": car
            })
        }
    except KeyError as e:
        return {
            "statusCode": 400,
            "body": json.dumps({
                "message": "BAD_REQUEST",
                "error": str(e)
            })
        }
    except Exception as e:
        logger.error("Unexpected error: %s", str(e), exc_info=True)
        return {
            "statusCode": 500,
            "body": json.dumps({
                "message": "INTERNAL_SERVER_ERROR",
                "error": str(e)
            })
        }


def save_car(car):
    connection = pymysql.connect(host=rds_host, user=rds_user, password=rds_password, db=rds_db)
    try:
        cursor = connection.cursor()
        cursor.execute(
            "INSERT INTO cars (marca, modelo, autonomia_electrica, autonomia_combustible) VALUES (%s, %s, %s, %s)",
            (car['marca'], car['modelo'], car['autonomia_electrica'], car['autonomia_combustible'])
        )
        connection.commit()
        return {
            "statusCode": 200,
            "body": json.dumps({
                "message": "Car saved successfully"
            })
        }

    except Exception as e:
        logger.error("Database transaction error: %s", str(e), exc_info=True)
        return {
            "statusCode": 500,
            "body": json.dumps({
                "message": "DATABASE_ERROR",
                "error": str(e)
            })
        }

    finally:
        connection.close()
