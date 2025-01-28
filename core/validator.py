import typing
import os

type ValidatorTypes = dict[typing.Optional[str], typing.Union[dict, typing.Callable]]


class ValidationError(Exception):
    pass


type ValidatorVariable = typing.Any
type ValidatorFunction = typing.Callable[[str, ValidatorVariable], ValidatorVariable]


# ToDo: Add support for lists
def validate_and_process_schema(schema: dict, data: dict, stack: list[str]) -> dict:
    for key, value in schema.items():
        if key == AnyName:
            for d_key, d_value in data.items():
                data[d_key] = validate_and_process_schema(schema[key], d_value, stack + [d_key])

        elif isinstance(value, dict):
            data[key] = validate_and_process_schema(schema[key], data.get(key), stack + [key])

        elif isinstance(value, list):
            ...

        elif isinstance(value, typing.Callable):
            v_function = schema[key]
            data[key] = v_function(key, data.get(key), stack)

        else:
            raise ValidationError("Corrupt configuration could not be interpreted!")

    return data


def optional(_type: ValidatorVariable, default: ValidatorVariable,
             func: typing.Callable = None) -> ValidatorFunction:
    def _inner(name: str, value: ValidatorVariable, stack: list[str]):
        try:
            if value is None:
                return default

            if not isinstance(value, _type):
                value = _type(value)

            # enrich environment variables
            if isinstance(value, str):
                value = os.path.expandvars(value)

            return value

        except ValueError:
            full_name = ".".join(stack + [name])
            raise ValidationError(
                f"Variable `{full_name}` with value `{value}` is of type `{type(value)}` and not convertable to `{_type}`!"
            )

    return _inner


def required(_type: typing.Type, func: typing.Callable = None) -> ValidatorFunction:
    def _inner(name: str, value: _type, stack: list[str]):
        try:
            if value is None:
                full_name = ".".join(stack + [name])
                raise ValidationError(
                    f"Variable `{full_name}` is required and cannot be `None`!"
                )

            if not isinstance(value, _type):
                value = _type(value)

            # enrich environment variables
            if isinstance(value, str):
                value = os.path.expandvars(value)

            return value

        except ValueError:
            full_name = ".".join(stack + [name])
            raise ValidationError(
                f"Variable `{full_name}` with value `{value}` is of type `{type(value)}` and not convertable to `{_type}`!"
            )

    return _inner


Optional = optional
Required = required


class AnyName:
    def __init__(self):
        raise ValidationError("Class `AnyName` is a type and cannot be called!")
