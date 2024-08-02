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
            logger.error("Esta solicitud requiere información para continuar")
            raise KeyError('body')

        body = json.loads(event['body'])
        car_id = body.get('id')

        if car_id is None:
            logger.error("El campo id es requerido para eliminar un carro")
            raise KeyError('id')

        if car_id == "" or not isinstance(car_id, int):
            logger.error("El campo id no puede estar vacío y debe ser un número entero")
            raise ValueError("El campo id no puede estar vacío")

        if get_car(car_id) is False:
            logger.error("El carro no existe")
            return {
                "statusCode": 404,
                "body": json.dumps({
                    "message": "CAR_NOT_FOUND"
                })
            }
        if is_already_disabled(car_id):
            logger.error("El carro ya está deshabilitado")
            return {
                "statusCode": 400,
                "body": json.dumps({
                    "message": "CAR_ALREADY_DISABLED"
                })
            }

        disable_car(car_id)
        return {
            "statusCode": 200,
            "body": json.dumps({
                "message": "CAR_DISABLED",
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


def get_car(car_id):
    connection = pymysql.connect(host=rds_host, user=rds_user, password=rds_password, db=rds_db)
    try:
        cursor = connection.cursor()
        cursor.execute(f"SELECT * FROM cars WHERE id = {car_id};")

        result = cursor.fetchone()

        if result is None:
            return False

        return True
    except Exception as e:
        logger.error("Database connection error: %s", str(e))
        return False
    finally:
        connection.close()


def is_already_disabled(car_id):

    connection = pymysql.connect(host=rds_host, user=rds_user, password=rds_password, db=rds_db)
    try:
        cursor = connection.cursor()
        cursor.execute()
        cursor.execute(f"SELECT * FROM cars WHERE id = {car_id};")

        result = cursor.fetchone()

        if result[0] == 0:
            return True

    except Exception as e:
        logger.error("Database connection error: %s", str(e))
        return {
            "statusCode": 500,
            "body": json.dumps({
                "message": "CONNECTION_ERROR"
            }),
        }
    finally:
        connection.close()


def disable_car(car_id):
    connection = pymysql.connect(host=rds_host, user=rds_user, password=rds_password, db=rds_db)
    try:
        cursor = connection.cursor()
        cursor.execute(f"UPDATE cars SET status = 0 WHERE id = {car_id};")
        connection.commit()

    except Exception as e:
        logger.error("Database connection error: %s", str(e))
        return {
            "statusCode": 500,
            "body": json.dumps({
                "message": "CONNECTION_ERROR"
            }),
        }
    finally:
        connection.close()

