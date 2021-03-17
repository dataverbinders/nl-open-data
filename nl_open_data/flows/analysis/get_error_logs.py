from prefect import Client


client = Client()

get_errors_query = """
query ($source: String) {
  flow_run(
    where: {state: {_eq: "Failed"}, name: {_ilike: $source}}
    order_by: {start_time: desc}
  ) {
    name
    start_time
    end_time
    logs(where: {level: {_eq: "ERROR"}}) {
      timestamp
      message
    }
  }
}
"""

query_variables = {"source": "cbs_v3%"}

result = client.graphql(get_errors_query, variables=query_variables)

flow_runs = result.data.flow_run

for flow in flow_runs:
    print()
    print("-" * 50)
    print(f"name: {flow.name}")
    print(f"start_time: {flow.start_time}")
    print(f"end_time: {flow.end_time}")
    print("-" * 50)
    print()
    print("First Error log")
    print("---------------")
    print(flow.logs[0])
    print()
    print("Last Error log")
    print("--------------")
    print(flow.logs[-1])
    print()
    # print("All Error Logs")
    # print("--------------")
    # for log in flow.logs:
    #     print("_" * 150)
    #     print()
    #     print(f"{log.timestamp}: {log.message}")
    #     print()
    #     print("_" * 150)
print()
print(f"{len(flow_runs)} failed flows")
print()
