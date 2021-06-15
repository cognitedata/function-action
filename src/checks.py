import ast
import contextlib
import logging

from config import FunctionConfig

logger = logging.getLogger(__name__)


HANDLE_ARGS = ("data", "client", "secrets", "function_call_info")


def run_checks(config: FunctionConfig) -> None:
    # Python-only checks:
    logger.info(f"Running checks on file '{config.function_file}'. Rest of config:")
    logger.info(str(config))
    # if config.function_file.endswith(".py"):
    _check_handle_args(config.function_file)


def _check_handle_args(filename: str, fn_name: str = "handle") -> None:
    with contextlib.suppress(FileNotFoundError):
        with open(filename) as file:
            file = file.read()
        for node in ast.walk(ast.parse(file)):
            if isinstance(node, ast.FunctionDef) and node.name == fn_name:
                bad_args = set(param.arg for param in node.args.args).difference(HANDLE_ARGS)
                if not bad_args:
                    return
                err_msg = (
                    f"In file '{file}', function '{fn_name}' contained illegal args: {list(bad_args)}. "
                    f"The function args must be a strict subset of: {list(HANDLE_ARGS)} "
                    "(ordering is not important)"
                )
                logger.error(err_msg)
                raise ValueError(err_msg)
