from prefect import Task, Flow


def my_state_handler(obj, old_state, new_state):
    msg = "\nCalling my custom state handler on {0}:\n{1} to {2}\n"
    print(msg.format(obj, old_state, new_state))
    if new_state.is_successful():
        print("SUCCESS")
    return new_state


my_flow = Flow(
    name="state-handler-demo", tasks=[Task()], state_handlers=[my_state_handler]
)

my_flow.run()
