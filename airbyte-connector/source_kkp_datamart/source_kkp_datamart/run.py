import sys
from airbyte_cdk.entrypoint import launch
from source_kkp_datamart import SourceKkpDatamart


def run():
    source = SourceKkpDatamart()
    launch(source, sys.argv[1:])


if __name__ == "__main__":
    run()
