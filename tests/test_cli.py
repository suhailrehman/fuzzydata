import pytest
from fuzzydata.cli import main


def test_main(tmpdir_factory):
    output_path = tmpdir_factory.mktemp('cli_test')
    args = [
        f"--wf_client=pandas",
        f"--output_dir={output_path}",
        "--columns=10",
        "--rows=100",
        "--version=5",
        "--matfreq=2"
    ]
    main(args)
