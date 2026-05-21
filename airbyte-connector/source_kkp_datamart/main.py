import sys
from source_kkp_datamart import SourceKkpDatamart
from airbyte_cdk.entrypoint import launch

if __name__ == "__main__":
    source = SourceKkpDatamart()
    launch(source, sys.argv[1:])
