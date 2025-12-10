import datetime
import typing

import dataclasses
import marshmallow
import decimal

from common import exceptions


def get_exception_message(exception: Exception) -> str:
    if isinstance(exception, type):
        return exception.__name__

    if hasattr(exception, "message") and exception.message:
        return exception.message

    return exception.args[0] if len(exception.args) else ""


def validate_data_schema(
    data: typing.Union[typing.Dict, typing.List[typing.Dict]],
    schema: marshmallow.schema.Schema,
) -> typing.Dict:
    try:
        validated_data = schema.load(data=data, unknown=marshmallow.EXCLUDE)
    except marshmallow.exceptions.ValidationError as e:
        raise exceptions.ValidationSchemaException(get_exception_message(exception=e))

    return validated_data


def convert_date(date_str: str) -> datetime.datetime | None:
    try:
        dt = datetime.datetime.strptime(date_str, "%d/%m/%Y")
        return dt.strftime("%Y-%m-%d; %H:%M")
    except (ValueError, TypeError):
        return None


def dataclass_to_dict(obj: typing.Any) -> typing.Any:
    if dataclasses.is_dataclass(obj):
        return {
            key: dataclass_to_dict(value)
            for key, value in dataclasses.asdict(obj).items()
        }
    elif isinstance(obj, (list, tuple)):
        return [dataclass_to_dict(item) for item in obj]
    elif isinstance(obj, decimal.Decimal):
        # Convert decimal to string to make it JSON serializable
        return str(obj)

    return obj