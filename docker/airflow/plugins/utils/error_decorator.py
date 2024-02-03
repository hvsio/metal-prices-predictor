from functools import wraps
from airflow.exceptions import AirflowFailException

def error_check(func):
    """Decorator function to raise AirflowException in case of encountered errors of any kind."""

    @wraps(func)
    def inner(*args):
        try:
            result = func(*args)
            return result
        except Exception as e:
            raise AirflowFailException(f'Encountered error in pipeline: {e}')
    return inner
