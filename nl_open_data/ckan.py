import requests


def get_datasets(base_url, search_terms, strict=False):
    if isinstance(search_terms, str):
        raise TypeError("search terms should be an iterable, not a string")
    action = "package_search"
    filter = f"?q={search_terms[0]}"
    for term in search_terms[1:]:
        filter += f"&q={term}"
    url_string = f"{base_url}/action/{action}{filter}"
    r = requests.get(url_string).json()
    if isinstance(r, str):
        print(r)
        return r
    elif not r["success"]:
        print("request to api did not succeed")
        return r
    else:
        datasets = (
            [result for result in r["result"]["results"] if result["type"] == "dataset"]
            if strict
            else [result for result in r["result"]["results"]]
        )
        return datasets
