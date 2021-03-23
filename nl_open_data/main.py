## NOT USING AT THE MOMENT - ADAPT OR DELETE!!

from prefect.engine.executors import DaskExecutor

from nl_open_data.config import get_config
from nl_open_data.flows.cbs import regionaal


executor = DaskExecutor(n_workers=8)


def main(config):
    regionaal.main.run(config=config)


if __name__ == "__main__":
    config = get_config("dataverbinders")
    main(config=config)

