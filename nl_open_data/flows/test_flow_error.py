import inspect
from prefect import task, Flow, Parameter

# # Name imports
# from nl_open_data.tasks import create_dir, remove_dir_name_import

# Module import
import nl_open_data.tasks as nlt

# # With Name imports
# create_dir = task(create_dir)
# remove_dir_name_import = task(remove_dir_name_import, skip_on_upstream_skip=False)

# With module imports
for name, _ in inspect.getmembers(nlt, inspect.isfunction):
    exec(f"nlt.{name} = task(nlt.{name})")
nlt.remove_dir_name_import.skip_on_upstream_skip = False


with Flow("impor_error") as flow:
    test_folder = Parameter("test_folder")

    # # With name imports
    # dir = create_dir(test_folder)
    # no_dir = remove_dir_name_import(dir, upstream_tasks=[dir])

    # With module imports
    dir = nlt.create_dir(test_folder)
    no_dir = nlt.remove_dir_name_import(dir, upstream_tasks=[dir])

if __name__ == "__main__":
    state = flow.run(parameters={"test_folder": "TEST_FOLDER"})
