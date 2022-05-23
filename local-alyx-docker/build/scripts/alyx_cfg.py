"""Alyx Configuration

This serves as a tool to setup Alyx/Django/Apache configuration files and.

Example:
    Usage as a console script:

        {prog} --help
        {prog} settings --help
        {prog} settings seceret --help
        {prog} --version
        {prog} -vv -d {alyx_src_path} -e KEY=value settings secret

        {prog} --env-file=.env apache -s 000-default.conf ip_whitelist.conf /etc/.../sites-available

Notes:
    - Python>=3.9 required
"""

import argparse
import json
import logging
import os
import re
import sys
from pathlib import Path
from typing import Any, Optional, Sequence, TypedDict, Union

StrPath = Union[str, Path]


class T_SettingsDefaults(TypedDict):
    alyx_instance: str
    alyx_network: str
    alyx_src_path: StrPath
    alyx_log_root: StrPath
    apache_confg_default_target: StrPath
    apache_confg_alyx_target: StrPath
    apache_confg_wl_target: StrPath
    pgdatabase: str
    pguser: str
    pgreadonly: str
    templates: list[str]
    write_mode: str
    chmod: str


VERSION: str = "0.0.1"
PROG: str = "alyx_cfg"
SETTINGS_DEFAULTS: T_SettingsDefaults = {
    "alyx_instance": os.getenv("ALYX_INSTANCE", "local"),
    "alyx_network": os.getenv("ALYX_NETWORK", "alyx-apache"),
    "alyx_src_path": f'/var/www/alyx-{os.getenv("ALYX_INSTANCE", "local")}',
    "alyx_log_root": "/var/log",
    "apache_confg_default_target": "/etc/apache2/apache2.conf",
    "apache_confg_alyx_target": "/etc/apache2/sites-available/000-default.conf",
    "apache_confg_wl_target": "/etc/apache2/sites-available/ip_whitelist.conf",
    "pgdatabase": os.getenv("PGDATABASE", "alyxdb"),
    "pguser": os.getenv("PGUSER", "ibl_dev"),
    "pgreadonly": os.getenv("PGREADONLY", "off"),
    "templates": ["main", "lab", "secret"],
    "write_mode": "w",
    "chmod": "775",
}
SECRETS: tuple[str, ...] = (
    "DJANGO_SECRET_KEY",
    "PGPASSWORD",
    "ALYX_ADMIN_PASS",
    "FLATIRON_SERVER_PWD",
)

log: logging.Logger = logging.getLogger(PROG)


def parse_args(args: Sequence[str]) -> argparse.Namespace:
    """Parse sys argument list then return parsed namespace object.

    Args:
        args (Sequence[str]): List of arguments to be passed in.

    Returns:
        argparse.Namespace: Namespace of parsed arguments, or will exit if passing
            help or version options.
    """

    class ArgumentDefaultsRawDescriptionHelpFormatter(
        argparse.RawDescriptionHelpFormatter,
        argparse.ArgumentDefaultsHelpFormatter,
    ):
        """Combination of different formatters."""

    class EnvVar(argparse.Action):
        """Split KEY=VALUE input arguments."""

        def __init__(self, option_strings, dest, nargs=None, **kwargs):
            if nargs is not None:
                raise ValueError("nargs not allowed")
            super().__init__(option_strings, dest, **kwargs)

        def __call__(
            self, parser, namespace, values: str, option_string: Optional[str] = None
        ):
            keyval = values.split("=")
            kwargs = getattr(namespace, self.dest)
            kwargs = {} if kwargs is None else kwargs
            kwargs.update({keyval[0]: keyval[1]})
            setattr(namespace, self.dest, kwargs)

    parser = argparse.ArgumentParser(
        prog=PROG,
        description=str(__doc__).format(
            prog=PROG, alyx_src_path=SETTINGS_DEFAULTS["alyx_src_path"]
        ),
        formatter_class=ArgumentDefaultsRawDescriptionHelpFormatter,
    )

    parser.add_argument(
        "-V", "--version", action="version", version=f"%(prog)s {VERSION}"
    )

    parser.add_argument("--verbose", "-v", action="count", default=0)

    parser.add_argument(
        "-d",
        "--directory",
        dest="source_directory",
        help="root path of the Alyx repo content, "
        f'e.g., {SETTINGS_DEFAULTS["alyx_src_path"]}',
        type=str,
        metavar="path",
        default=SETTINGS_DEFAULTS["alyx_src_path"],
    )

    parser.add_argument(
        "--env-file",
        dest="env_file",
        help="specify an environment file to load",
        type=str,
        metavar="path",
    )

    parser.add_argument(
        "--env",
        "-e",
        dest="env",
        help="specify a single environment variable to use",
        type=str,
        metavar="kwarg",
        action=EnvVar,
    )

    commands = parser.add_subparsers(
        dest="command",
        title="Alyx configuration commands",
        description="List commands to use along with the main "
        "'%(prog)s' command-line interface.",
        metavar="COMMAND",
        help="DESCRIPTION",
        required=False,
    )

    settings = commands.add_parser(
        "settings",
        description="Configure Alyx/Django settings files from the provided templates. "
        "See also %(prog)s --help",
        help="Configure Alyx/Django settings files from the provided templates.",
        aliases=["template"],
    )

    settings.add_argument(
        "-s",
        "--source-file",
        dest="source_file",
        help="Specify a source file to be configured instead of using the root "
        "directory option along with a subcommand. Must be a path and not a file name.",
        type=str,
        metavar="path",
    )

    settings.add_argument(
        "-t",
        "--target",
        dest="target_path",
        help="Manually set the target path to write to.",
        type=str,
        metavar="path",
    )

    settings.add_argument(
        "--set-all",
        dest="set_all",
        help="configure all settings template files using default options "
        "for each setting type",
        action="store_true",
    )

    settings.add_argument(
        "--target-mode",
        dest="target_mode",
        help="File open mode for written target file.",
        type=str,
        metavar="",
        default=SETTINGS_DEFAULTS["write_mode"],
    )

    settings.add_argument(
        "--chmod",
        dest="chmod",
        help="File permissions code.",
        type=str,
        metavar="",
        default=SETTINGS_DEFAULTS["chmod"],
    )

    subcommands = settings.add_subparsers(
        dest="settings_type",
        title="alyx settings types",
        description="a list of subcommands that come after the settings command",
        metavar="SUBCOMMAND",
        help="DESCRIPTION",
        required=False,
    )

    settings_parsers = {}
    for settings_type in SETTINGS_DEFAULTS["templates"]:
        description = (
            f"set the content of the settings_{settings_type}.py file".replace(
                "_main", ""
            )
        )
        settings_parsers[settings_type] = subcommands.add_parser(
            settings_type,
            description=description,
            help=description,
        )

    apache = commands.add_parser(
        "apache",
        description="Write the Apache configuration files. " "See also %(prog)s --help",
        help="Write the Apache configuration files.",
        aliases=["conf"],
    )

    apache.add_argument(
        "-s",
        "--source-file",
        dest="source_file",
        help="Specify a source file to be configured. May be used more "
        "than once. Must be a path and not a file name.",
        type=str,
        nargs="*",
        action="extend",
        metavar="path",
    )

    apache.add_argument(
        dest="target_path",
        help="The target path (directory or file) to write to.",
        type=str,
        metavar="target",
    )

    apache.add_argument(
        "--target-mode",
        dest="target_mode",
        help="File open mode for written target file.",
        type=str,
        metavar="",
        default=SETTINGS_DEFAULTS["write_mode"],
    )

    apache.add_argument(
        "--chmod",
        dest="chmod",
        help="File permissions code.",
        type=str,
        metavar="",
        default=SETTINGS_DEFAULTS["chmod"],
    )

    parsed_args = parser.parse_args(args)

    return parsed_args


def _python_logger(loglevel: int, base_level: str = "WARNING") -> logging.Logger:
    """Setup basic python logging

    Args:
        loglevel (int): Minimum log level number for emitting messages, e.g., '2'
        base_level (str, optional): Base level name for the logger.
            Defaults to "WARNING".

    Returns:
        logging.Logger: Updated module-level logger.
    """

    base_level = base_level.upper()

    if loglevel is None or not isinstance(loglevel, (float, int)):
        loglevel = logging.getLevelName(base_level)
    elif 10 > loglevel >= 1:
        base_loglevel = logging.getLevelName(base_level) / 10
        loglevel = max(int(base_loglevel - min(loglevel, base_loglevel)), 1) * 10
    elif loglevel % 10 == 0:
        loglevel = int(loglevel)
    else:
        loglevel = logging.getLevelName(base_level)

    std_out_handler = logging.StreamHandler(stream=sys.stdout)
    formatter = logging.Formatter(
        fmt="[%(asctime)s %(process)d %(processName)s "
        "%(levelname)s %(name)s]: %(message)s",
        datefmt="%z %Y-%m-%d %H:%M:%S",
    )

    log.handlers = []
    log.setLevel(loglevel)
    std_out_handler.setLevel(loglevel)
    std_out_handler.setFormatter(formatter)
    log.addHandler(std_out_handler)
    log.info(f"logging set to level: '{logging.getLevelName(loglevel)}'")

    return log


def _secret_key():
    """Create a Django secret key. Requires `django` module.

    Returns:
        _type_: Random string
    """
    try:
        from django.utils.crypto import get_random_string
    except ImportError as err:
        log.error(f"django not installed:\n{err}")
        return None
    chars = "abcdefghijklmnopqrstuvwxyz0123456789!@#$%^&*(-_=+)"
    return get_random_string(50, chars)


def _psecret(key: str, value: Any, *, secrets: Optional[tuple[str, ...]] = None) -> Any:
    secrets = secrets or SECRETS
    return "********" if key in secrets else value


def _tag(
    *args: str, prefix_: str = "%", suffix_: str = "%"
) -> Union[tuple[str, ...], str]:
    tagged = tuple(f"{prefix_}{v}{suffix_}" for v in args)
    if len(args) == 1:
        return tagged[0]
    return tagged


def _json_print(dict_: dict[str, object]) -> str:
    """Pretty-print basic dictionary using json.

    Args:
        dict_ (dict[str, object]): A dictionary with standard value types and
            string keys.

    Returns:
        str: String of dictionary object.
    """

    return "\n" + json.dumps({k: _psecret(k, v) for k, v in dict_.items()}, indent=4)


def _rm_empty(dict_: dict[Any, Any]) -> dict[Any, Any]:
    """Return a dictionary without the empty items, determined by an
    objects __bool__ method. Non-recursive.

    Args:
        dict_ (dict[Any, Any]): A dictionary.

    Returns:
        dict[Any, Any]: A dictionary subset.
    """
    return {k: v for k, v in dict_.items() if v}


def _path_file(path: StrPath, must_exist=False) -> Path:
    """File path resolver

    Args:
        path (StrPath): Path-like object passed to `Path`
        must_exist (bool, optional): Check if path exists. Defaults to False.

    Raises:
        FileExistsError: Path doesn't exist

    Returns:
        Path: Path to a file.
    """
    file = Path(path).expanduser().resolve()
    if must_exist and not (file.is_file() and file.exists()):
        raise FileExistsError(f"file doesn't exist '{file}'")
    return file


def _path_dir(path: StrPath, must_exist=False) -> Path:
    """Folder path resolver

    Args:
        path (StrPath): Path-like object passed to `Path`
        must_exist (bool, optional): Check if path exists. Defaults to False.

    Raises:
        NotADirectoryError: Path doesn't exist

    Returns:
        Path: Path to a directory.
    """
    folder = Path(path).expanduser().resolve()
    if must_exist and not (folder.is_dir() and folder.exists()):
        raise NotADirectoryError(f"folder doesn't exist '{folder}'")
    return folder


def _read_file(
    file: StrPath,
    *,
    comment: str = "#",
    remove_empty: bool = False,
    as_list: bool = True,
) -> Union[str, list[str]]:
    """Read a file with comments and return as a string or list

    Args:
        file (StrPath): file path
        comment (str, optional): comment string. Defaults to "#".
        remove_empty (bool, optional): delete empty lines. Defaults to False.
        as_list (bool, optional): return a list of lines instead of one string.
            Defaults to True.

    Returns:
        Union[str, list[str]]: string or list of strings
    """
    file = _path_file(file, must_exist=True)
    text = []
    with open(file, "r") as fc:
        for line in fc:
            if line.startswith(comment):
                continue
            if remove_empty and not line.strip():
                continue
            text.append(line)
    if not as_list:
        text = "".join(text)
    return text


def _touch_file(file: StrPath, chmod: int = 0o664):
    file = _path_file(file)
    try:
        file.parent.mkdir(parents=True, exist_ok=True)
        os.chmod(file.parent, 0o775)
        file.touch(exist_ok=True)
        if chmod:
            os.chmod(file, chmod)
    except Exception as err:
        log.warning(f"Failed to create file: {file}\n{err}")


def _replace_in_file(
    source_file: StrPath,
    target_file: StrPath,
    replacements: Optional[dict[str, str]] = None,
    target_mode: str = "w",
    chmod: Optional[int] = None,
) -> None:
    source_file = _path_file(source_file, must_exist=True)
    target_file = _path_file(target_file)

    if not target_file.parent.exists():
        target_file.parent.mkdir(0o775, True, True)

    replacements = {} if replacements is None else replacements

    log.debug(f"reading file: {source_file}")
    with open(source_file, "r") as f:
        contents = f.read()

    for key, value in replacements.items():
        if key in contents:
            log.debug(
                f"replacing '{key}' with "
                f"'{_psecret(key, value, secrets=_tag(*SECRETS))}'"
            )
            contents = contents.replace(key, value)

    log.debug(f"writing replacements text to file: {target_file}")
    with open(target_file, target_mode) as f:
        f.write(contents)

    if chmod is not None:
        log.debug(f"Updating target file permissions <{oct(chmod)}>: {target_file}")
        os.chmod(target_file, chmod)


class EnvironVars:
    def __init__(self) -> None:
        self.default_vars = {}
        self.environ_vars = {}
        self.dotfile_vars = {}
        self.environment = self._load_defaults()
        self.environment |= self._load_environ_vars(*self.default_vars.keys())

    def load_vars(
        self,
        env_file: Optional[StrPath] = None,
        *args: str,
        **kwargs: str,
    ) -> dict[str, str]:
        """Get variables from various sources.

        Variables are sourced in the following order:

            - default values set in this file
            - system environment versions of default variables pulled from `os.environ`
            - variables defined with the `env_file` cli option
            - additional `os.environ` variables to try and get
            - individual variables defined with the `env` cli option

        Returns:
            dict[str,str]: dictionary of replacement values
        """

        self.environment |= self._load_dotfile_vars(env_file)
        self.environment |= self._load_environ_vars(
            *[a for a in args if a and isinstance(a, str)]
        )
        self.environment |= _rm_empty(kwargs)
        self.environment = _rm_empty(self.environment)

        log.debug("%s%s", "all variables loaded:", str(self))

        return {_tag(e): v for e, v in self.environment.items()}

    def check_missing(self, *args, raise_error=True):
        if missing := tuple(
            k for k in args if k not in self.environment or not self.environment.get(k)
        ):
            if raise_error:
                raise KeyError(f"missing {missing} from: {args}")
        return missing

    def _load_defaults(self):
        self.default_vars = {
            "DJANGO_SECRET_KEY": _secret_key(),
            "PGDATABASE": SETTINGS_DEFAULTS["pgdatabase"],
            "PGUSER": SETTINGS_DEFAULTS["pguser"],
            "PGREADONLY": SETTINGS_DEFAULTS["pgreadonly"],
            "PGPASSWORD": None,
            "ALYX_INSTANCE": SETTINGS_DEFAULTS["alyx_instance"],
            "ALYX_LOG_FILE": (
                _path_dir(SETTINGS_DEFAULTS["alyx_log_root"]) / "alyx.log"
            ).as_posix(),
            "ALYX_JSON_LOG_FILE": (
                _path_dir(SETTINGS_DEFAULTS["alyx_log_root"]) / "alyx_json.log"
            ).as_posix(),
        }
        log.debug("%s%s", "vars (default values):", _json_print(self.default_vars))
        return self.default_vars

    def _load_environ_vars(self, *args):
        self.environ_vars = _rm_empty({arg: os.getenv(arg) or None for arg in args})
        log.debug("%s%s", "vars (environ values):", _json_print(self.environ_vars))
        return self.environ_vars

    def _load_dotfile_vars(self, env_file: Optional[StrPath]) -> dict[str, str]:
        self.dotfile_vars = {}
        if not env_file:
            return self.dotfile_vars
        text = _read_file(env_file, remove_empty=True, as_list=True)
        for line in text:
            key, value = line.strip().split("=", 1)
            value = self._strip_quoted(value)
            self.dotfile_vars[key] = value
        self.dotfile_vars = _rm_empty(self.dotfile_vars)
        log.debug("%s%s", "vars (.env file values):", _json_print(self.dotfile_vars))
        return self.dotfile_vars

    @staticmethod
    def _strip_quoted(value: str) -> str:
        if (value.startswith('"') and value.endswith('"')) or (
            value.startswith("'") and value.endswith("'")
        ):
            value = value[1:-1]
        return value

    def __str__(self) -> str:
        return _json_print(self.environment)

    def __repr__(self) -> str:
        return str(self)


class Settings(EnvironVars):
    def __init__(
        self,
        *args,
        **kwargs,
    ) -> None:
        super().__init__()
        self.settings_type = kwargs.get("settings_type")
        self.source_file = kwargs.get("source_file")
        self.target_path = kwargs.get("target_path")
        self.source_directory = kwargs.get("source_directory")
        self.env_file = kwargs.get("env_file")
        self.other_vars = args
        self.env = kwargs.get("env")
        self.target_mode = kwargs.get("target_mode", SETTINGS_DEFAULTS["write_mode"])
        chmod = kwargs.get("chmod", SETTINGS_DEFAULTS["chmod"])
        self.chmod = int(chmod, base=8) if chmod else None
        self.replacements = self.load_vars(self.env_file, *self.other_vars, **self.env)

    def write_files(self) -> None:
        source_file, target_file = self._get_source_and_target()
        self._check_missing_vars()
        self._touch_log_files()
        _replace_in_file(
            source_file=source_file,
            target_file=target_file,
            replacements=self.replacements,
            target_mode=self.target_mode,
            chmod=self.chmod,
        )

    def _get_template_type(self, file: Optional[StrPath] = None) -> Optional[str]:
        type_ = self.settings_type or (Path(file).name if file else "")
        if not (
            match := re.search(
                r"""
                (?P<lab>_lab_|_lab\.py$|^lab$)                          # lab
                |(?P<secret>_secret_|_secret\.py$|^secret$)             # secret
                |(?P<main>settings_template\.py|^settings\.py$|^main$)  # main
                """,
                type_,
                re.VERBOSE,
            )
        ):
            return None
        matched_groups = _rm_empty(match.groupdict())
        return set(matched_groups.keys()).pop() if matched_groups else None

    def _set_template_type_from_files(
        self,
        source_file: Optional[StrPath] = None,
        target_file: Optional[StrPath] = None,
    ) -> None:
        types = list(filter(None, [self.settings_type]))
        if not types:
            if source_file:
                types.append(self._get_template_type(source_file))
            if target_file:
                types.append(self._get_template_type(target_file))
        types = set(filter(None, types))
        if len(types) != 1:
            raise ValueError(
                "Settings subcommand/type name mismatch or was empty. Must be one of: "
                f'{SETTINGS_DEFAULTS["templates"]}, '
                f"found {[*types] or '<empty>'}"
            )
        settings_type = types.pop()
        if settings_type not in SETTINGS_DEFAULTS["templates"]:
            raise ValueError(
                "Settings subcommand/type must be one of: "
                f'{SETTINGS_DEFAULTS["templates"]}, '
                f"found {settings_type}"
            )
        self.settings_type = settings_type

    def _get_paths(
        self,
    ) -> tuple[Optional[Path], Optional[Path], Optional[Path], Optional[Path]]:
        source_directory = None
        source_file = None
        target_directory = None
        target_file = None

        if self.source_file:
            source_file = _path_file(self.source_file, must_exist=True)
        else:
            source_directory = _path_dir(self.source_directory, must_exist=True)

        if self.target_path:
            try:
                target_directory = _path_dir(self.target_path, must_exist=True)
            except NotADirectoryError:
                target_file = _path_file(self.target_path)

        return source_directory, source_file, target_directory, target_file

    def _get_source_and_target(self) -> tuple[Path, Path]:
        source_directory, source_file, target_directory, target_file = self._get_paths()

        self._set_template_type_from_files(source_file, target_file)

        if not target_file and target_directory:
            target_name = f"settings_{self.settings_type}.py".replace("_main", "")
            target_file = _path_file(Path(target_directory) / target_name)

        if source_file and target_file:
            log.debug(f"\nsource_file: '{source_file}'\ntarget_file: '{target_file}'")
            return Path(source_file), Path(target_file)

        if not source_directory:
            raise FileNotFoundError(
                "alyx source path is required if "
                "both source and target files are not manually specified"
            )

        if (source_directory / "setup.py").exists():
            source_directory = source_directory / "alyx" / "alyx"

        if not source_file:
            flist = [
                source_directory / f"settings_{t}_template.py"
                for t in SETTINGS_DEFAULTS["templates"]
                if t == self.settings_type
            ]
            source_file = flist.pop()
            source_file = _path_file(
                source_file.as_posix().replace("_main_", "_"), must_exist=True
            )

        if not target_file:
            target_name = f"settings_{self.settings_type}.py".replace("_main", "")
            target_file = source_directory / target_name

        log.debug(f"\nsource_file: '{source_file}'\ntarget_file: '{target_file}'")

        if not (source_file and target_file):
            raise FileNotFoundError(
                "source file and target file could not be determined"
            )

        return Path(source_file), Path(target_file)

    def _check_missing_vars(self) -> None:
        if not self.settings_type:
            return
        required = {
            "main": (
                "ALYX_JSON_LOG_FILE",
                "ALYX_LOG_FILE",
                "ALYX_INSTANCE",
            ),
            "lab": (
                "PGUSER",
                "ALYX_NETWORK",
            ),
            "secret": (
                "DJANGO_SECRET_KEY",
                "PGPASSWORD",
                "PGDATABASE",
                "PGUSER",
                "PGHOST",
            ),
        }
        self.check_missing(*required[self.settings_type])

    def _touch_log_files(self) -> None:
        if (
            not self.replacements
            or not self.settings_type
            or self.settings_type != "main"
        ):
            return
        if "%ALYX_LOG_FILE%" in self.replacements:
            _touch_file(self.replacements["%ALYX_LOG_FILE%"])
        if "%ALYX_JSON_LOG_FILE%" in self.replacements:
            _touch_file(self.replacements["%ALYX_JSON_LOG_FILE%"])


def configure_settings(args: argparse.Namespace) -> None:
    kwargs = vars(args)
    types = (
        SETTINGS_DEFAULTS["templates"]
        if args.set_all
        else [kwargs.get("settings_type", None)]
    )
    kwargs["settings_type"] = None
    settings = Settings(**kwargs)
    for type_ in types:
        settings.settings_type = type_
        settings.write_files()
    log.info("Settings configure complete.")


class Apache(EnvironVars):
    def __init__(
        self,
        *args,
        **kwargs,
    ) -> None:
        super().__init__()
        self.source_directory = kwargs.get("source_directory")
        self.source_files: list[Path] = self._get_src_files(kwargs.get("source_file"))
        self.target_files = self._get_target_files(kwargs.get("target_path"))
        self.env_file = kwargs.get("env_file")
        self.other_vars = args
        self.env = kwargs.get("env")
        self.target_mode = kwargs.get("target_mode", SETTINGS_DEFAULTS["write_mode"])
        chmod = kwargs.get("chmod", SETTINGS_DEFAULTS["chmod"])
        self.chmod = int(chmod, base=8) if chmod else None
        self.replacements = self.load_vars(self.env_file, *self.other_vars, **self.env)

    def _get_src_files(
        self, source_files: Optional[Union[str, list[str]]] = None
    ) -> list[Path]:
        if source_files:
            if isinstance(source_files, str):
                source_files = [source_files]
            sources = [_path_file(src, must_exist=True) for src in source_files]
            return sources
        source_directory = _path_dir(self.source_directory, must_exist=True)
        sources = list(source_directory.glob("*.conf"))
        sources.extend(list(source_directory.glob("*.json")))
        return sources

    def _get_target_files(self, target_path: str) -> list[Path]:
        try:
            target_directory = _path_dir(target_path, must_exist=True)
            target_files = [
                target_directory / Path(src).name for src in self.source_files
            ]
        except NotADirectoryError:
            target_files = [_path_file(target_path)]

        if len(self.source_files) != len(target_files):
            raise ValueError(
                "Matching source and target files not found."
                f"\nsource({len(self.source_files)}): {self.source_files}"
                f"\ntarget({len(target_files)}): {target_files}"
            )
        return target_files

    def write_files(self) -> None:
        for source_file, target_file in zip(self.source_files, self.target_files):
            _replace_in_file(
                source_file=source_file,
                target_file=target_file,
                replacements=self.replacements,
                target_mode=self.target_mode,
                chmod=self.chmod,
            )


def configure_apache(args: argparse.Namespace) -> None:
    kwargs = vars(args)
    apache = Apache(**kwargs)
    apache.write_files()
    log.info("Apache configure complete.")


def run(args: argparse.Namespace):
    """Main function to run setup routines.

    Args:
        args (argparse.Namespace): Parsed arguments.

    Raises:
        SystemError: Eempty command name provided.
        NotImplementedError: Wrong command name likely entered.
        SystemExit: Exit after running commands.
    """
    _python_logger(args.verbose)

    if args.command is None:
        raise SystemError("must provide a valid COMMAND to run, e.g., 'settings'")
    elif args.command == "settings":
        configure_settings(args)
    elif args.command == "apache":
        configure_apache(args)
    else:
        raise NotImplementedError(f"Invalid command name '{args.command}'")

    raise SystemExit


def cli() -> None:
    """Calls this program, passing along the cli arguments extracted from `sys.argv`.

    This function can be used as entry point to create console scripts.
    """
    sys_args = sys.argv[1:]
    args = parse_args(sys_args or ["-h"])
    run(args)


if __name__ == "__main__":
    cli()
