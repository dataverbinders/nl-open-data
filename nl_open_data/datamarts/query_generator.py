from nl_open_data.config import config

# from nl_open_data.config import get_config
from google.cloud import bigquery


def get_dimensions_from_bq(id, schema="cbs", credentials=None, GCP=None):
    """Query dataset for its dimensions

    For given dataset id, all its dimensions are queried.
    Possible dimension types:
    - Dimension
    - TimeDimension
    - GeoDimension
    - GeoDetail

    For more details: https://www.cbs.nl/-/media/statline/documenten/handleiding-cbs-opendata-services.pdf?la=nl-nl
    """

    # initialize client
    bq = bigquery.Client(
        credentials=credentials, project=GCP.project_id, location=GCP.location
    )

    # prepare sql query text
    query = f"""
    SELECT Key, Title, Type
    FROM {GCP.project_id}.{schema}.{id}_DataProperties
    WHERE Type LIKE '%Dimension%' OR Type LIKE '%Geo%'
    """
    # execute query
    query_job = bq.query(query)
    return query_job


def get_topics_from_bq(id, schema="cbs", credentials=None, GCP=None):
    """Query dataset for its topics

    For given dataset id, all its topics are queried.

    For more details: https://www.cbs.nl/-/media/statline/documenten/handleiding-cbs-opendata-services.pdf?la=nl-nl
    """

    # initialize client
    bq = bigquery.Client(credentials=credentials, project=GCP.project_id)

    # prepare sql query text
    query = f"""
    SELECT Key, Title, Type
    FROM {GCP.project_id}.{schema}.{id}_DataProperties
    WHERE Type LIKE '%Topic%'
    """
    # execute query
    query_job = bq.query(query)
    return query_job


def write_select_dimensions(dims_dict):
    """Create a string for a SELECT part of an SQL query for dimension tables

    Given a dictionary key-value pairs, this function outputs a string to be
    used as part of an SQL SELECT section. This is meant to be used when
    flatenning a table, and the given dict should contain all Key-Title pairs
    of the relevant dimensions.

    For more details: https://www.cbs.nl/-/media/statline/documenten/handleiding-cbs-opendata-services.pdf?la=nl-nl
    """

    string = ""
    for i, (key, title) in enumerate(dims_dict.items()):
        if i == 0:
            string += (
                f" {key}.Key AS {title.lower().replace(' ', '_')}_code"  # no comma for first item
                f"\n    , {key}.Title AS {title.lower().replace(' ', '_')}"
            )
        else:
            string += (
                f"\n    , {key}.Key AS {title.lower().replace(' ', '_')}_code"
                f"\n    , {key}.Title AS {title.lower().replace(' ', '_')}"
            )
    return string


def write_select_topics(topics_dict):
    """Create a string for a SELECT part of an SQL query for a fact table

    Given a dictionary key-value pairs, this function outputs a string to be
    used as part of an SQL SELECT section. This is meant to be used when
    flatenning a table, and the given dict should contain all Key-Title pairs
    of the fact table to be used.

    For more details: https://www.cbs.nl/-/media/statline/documenten/handleiding-cbs-opendata-services.pdf?la=nl-nl
    """

    string = ""
    for key, title in topics_dict.items():
        string += f"\n    , fct.{key} AS {title.lower().replace(' ', '_').replace('(', '').replace(')', '').replace('%', 'per').replace(',', '')}"
    return string


def write_join_dimensions(dims_dict, join_type, id, schema, GCP):
    """Creates the join section of an sql query for dimension tables

    Given a dictionary key-value pairs, this function outputs a string to be
    used as part of an SQL JOIN section. This is meant to be used when
    flatenning a table, and the given dict should contain all Key-Title pairs
    of the relevant dimensions.

    For more details: https://www.cbs.nl/-/media/statline/documenten/handleiding-cbs-opendata-services.pdf?la=nl-nl
    """

    if join_type.upper() not in ["INNER", "LEFT", "RIGHT", "FULL"]:
        print('join_type must be one of: "INNER", "LEFT", "RIGHT", "FULL"')
        return None
    else:
        string = ""
        for key, title in dims_dict.items():
            string += f"\n  {join_type.upper()} JOIN {GCP.project_id}.{schema}.{id}_{key} AS {key} ON {key}.key = fct.{key}"
    return string


def flatten_table(id, join_type="INNER", schema="cbs", credentials=None, GCP=None):
    """Flatten a table by joining a fact table (TypedDataSet) with its
    corresponding dimension tables.
    """
    # get title
    # title = short title from TableInfos? From user? Other idea?
    title = "Insert Title Here"  # temp - use static TODO
    title = title.lower().replace(" ", "_")  # pythonize title string

    # get dimension info
    dims_query = get_dimensions_from_bq(
        id=id, schema=schema, credentials=credentials, GCP=GCP
    )

    # place dimensions in dicts according to type / ALTERNATIVE OPTION - one iterable with 'Type' marked per item?
    # dim_types = ["Dimension", "TimeDimension", "GeoDimension", "GeoDetail"]
    dims = {
        row["Key"]: row["Title"] for row in dims_query if row["Type"] == "Dimension"
    }
    time_dims = {
        row["Key"]: row["Title"] for row in dims_query if row["Type"] == "TimeDimension"
    }
    geo_dims = {
        row["Key"]: row["Title"] for row in dims_query if row["Type"] == "GeoDimension"
    }
    geo_details = {
        row["Key"]: row["Title"] for row in dims_query if row["Type"] == "GeoDetail"
    }

    # get topics info
    topics_query = get_topics_from_bq(
        id=id, schema=schema, credentials=credentials, GCP=GCP
    )

    # place topics in a list
    topics = {row["Key"]: row["Title"] for row in topics_query}

    # CREATE statement
    create = f"CREATE OR REPLACE TABLE {GCP.project_id}.dso.{title}"

    # PARTITION statement #TODO - how to decide on times?? Is it always years?
    partition = ""

    # SELECT statement
    select = "\n  SELECT" + write_select_dimensions(dims)
    select += write_select_topics(topics)

    # FROM statement
    from_statement = f"\n  FROM {GCP.project_id}.{schema}.{id}_TypedDataSet AS fct"

    # JOIN statement
    join = write_join_dimensions(
        dims_dict=dims, join_type="INNER", id=id, schema=schema, GCP=GCP
    )

    # concat query
    query = create + partition + " AS (" + select + from_statement + join + "\n)"

    return query

    # # initialize client
    # bq = bigquery.Client(credentials=credentials, project=GCP.project_id)

    # # configure job
    # job_config = bigquery.QueryJobConfig(destination=table_id)

    # # execute query
    # query_job = bq.query(query)
    # return query_job


def main(GCP):
    query = flatten_table(
        id=table_id, join_type="inner", schema=schema, credentials=None, GCP=GCP
    )
    print(query)

    # dims_query = get_dimensions_from_bq(
    #     id=table_id,
    #     schema='mlz',
    #     credentials=None,
    #     GCP=my_gcp
    #     )

    # # place dimensions in dicts according to type ALTERNATIVE OPTION - one iterable with 'Type' marked per item?
    # dims = {row['Key']: row['Title'] for row in dims_query if row['Type']=="Dimension"}
    # time_dims = {row['Key']: row['Title'] for row in dims_query if row['Type']=="TimeDimension"}
    # geo_dims = {row['Key']: row['Title'] for row in dims_query if row['Type']=="GeoDimension"}
    # geo_details = {row['Key']: row['Title'] for row in dims_query if row['Type']=="GeoDetail"}
    # print(write_join_dimensions(dims, "INNER", table_id, schema, my_gcp))


# for local testing purposes
if __name__ == "__main__":
    # # config = get_config("ag")
    my_gcp = config.gcp.dev
    table_id = "83674NED"
    schema = "cbs_v3_83674NED"
    main(my_gcp)

    # data_properties = get_dimensions_from_bq(id=table_id, GCP=my_gcp, schema=schema)

    # print(f"The dimensions for table {table_id}:")
    # for row in data_properties:
    #     print(f"Key = {row['Key']}, Title={row['Title']}, Type={row['Type']}")
