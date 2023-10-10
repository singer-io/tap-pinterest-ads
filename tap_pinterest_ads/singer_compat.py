from typing import Any, Callable, Dict, List, Optional, Tuple, Type, Union, cast
from pathlib import Path, PurePath
from singer_sdk.helpers._classproperty import classproperty
import click


class SingerCompatibilityMixin:

    @classproperty
    def cli(cls) -> Callable:
        """Execute standard CLI handler for taps.

        Returns:
            A callable CLI object.
        """

        @click.option(
            "--version",
            is_flag=True,
            help="Display the package version.",
        )
        @click.option(
            "--about",
            is_flag=True,
            help="Display package metadata and settings.",
        )
        @click.option(
            "--format",
            help="Specify output style for --about",
            type=click.Choice(["json", "markdown"], case_sensitive=False),
            default=None,
        )
        @click.option(
            "--discover",
            is_flag=True,
            help="Run the tap in discovery mode.",
        )
        @click.option(
            "--test",
            is_flag=True,
            help="Test connectivity by syncing a single record and exiting.",
        )
        @click.option(
            "--config",
            multiple=True,
            help="Configuration file location or 'ENV' to use environment variables.",
            type=click.STRING,
            default=(),
        )
        @click.option(
            "--properties",
            multiple=True,
            help="Configuration file location or 'ENV' to use environment variables.",
            type=click.STRING,
            default=(),
        )
        @click.option(
            "--catalog",
            help="Use a Singer catalog file with the tap.",
            type=click.Path(),
        )
        @click.option(
            "--state",
            help="Use a bookmarks file for incremental replication.",
            type=click.Path(),
        )
        @click.command(
            help="Execute the Singer tap.",
            context_settings={"help_option_names": ["--help"]},
        )
        def cli(
            version: bool = False,
            about: bool = False,
            discover: bool = False,
            test: bool = False,
            config: Tuple[str, ...] = (),
            state: str = None,
            catalog: str = None,
            properties: str = None,
            format: str = None,
        ) -> None:
            """Handle command line execution.

            Args:
                version: Display the package version.
                about: Display package metadata and settings.
                discover: Run the tap in discovery mode.
                test: Test connectivity by syncing a single record and exiting.
                format: Specify output style for `--about`.
                config: Configuration file location or 'ENV' to use environment
                    variables. Accepts multiple inputs as a tuple.
                catalog: Use a Singer catalog file with the tap.",
                state: Use a bookmarks file for incremental replication.

            Raises:
                FileNotFoundError: If the config file does not exist.
            """
            if version:
                cls.print_version()
                return

            if not about:
                cls.print_version(print_fn=cls.logger.info)

            validate_config: bool = True
            if about or discover:
                # Don't abort on validation failures
                validate_config = False

            parse_env_config = False
            config_files: List[PurePath] = []
            for config_path in config:
                if config_path == "ENV":
                    # Allow parse from env vars:
                    parse_env_config = True
                    continue

                # Validate config file paths before adding to list
                if not Path(config_path).is_file():
                    raise FileNotFoundError(
                        f"Could not locate config file at '{config_path}'."
                        "Please check that the file exists."
                    )

                config_files.append(Path(config_path))

            tap = cls(  # type: ignore  # Ignore 'type not callable'
                config=config_files or None,
                state=state,
                catalog=catalog,
                parse_env_config=parse_env_config,
                validate_config=validate_config,
            )
            if about:
                tap.print_about(format)
            elif discover:
                tap.run_discovery()
                if test:
                    tap.run_connection_test()
            elif test:
                tap.run_connection_test()
            else:
                tap.sync_all()

        return cli
