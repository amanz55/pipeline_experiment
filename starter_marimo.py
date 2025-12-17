import marimo

__generated_with = "0.18.4"
app = marimo.App(width="medium")


@app.cell
def _():
    import duckdb
    import dlt
    return dlt, duckdb


@app.cell
def _():
    import marimo as mo
    return (mo,)


@app.cell
def _():
    from dlt.sources.filesystem import filesystem
    return (filesystem,)


@app.cell
def _(filesystem):
    resource = filesystem(
        bucket_url="Parquets/d_mda_state_INC_d20240729224455_f49f9b7c-8d51-4fde-a3d1-f0422220380d_0.parquet",
        # file_glob="*.parquet"
    )
    return (resource,)


@app.cell
def _(resource):
    print(resource)
    return


@app.cell
def _(dlt):
    pipeline = dlt.pipeline(
        dev_mode=True,
        pipelines_dir=".dlt\pipelines",
        pipeline_name="filesystem_example",
        destination="duckdb",
        dataset_name="filesystem_data",
    )
    return (pipeline,)


@app.cell
def _(pipeline, resource):
    load_info = pipeline.run(resource)
    return (load_info,)


@app.cell
def _(load_info):
    print(load_info)
    return


@app.cell
def _(duckdb):
    con = duckdb.connect(database='filesystem_example.duckdb', read_only=False)
    return (con,)


@app.cell
def _(con):
    con.sql("show all tables").show()
    return


@app.cell
def _(con):
    df = con.sql("show all tables")
    return (df,)


@app.cell
def _(df, mo):
    mo.ui.table(df)
    return


@app.cell
def _(con):
    df2 = con.sql("select * from filesystem_data_20251217081154._dlt_loads")
    return (df2,)


@app.cell
def _(df2, mo):
    mo.ui.table(df2)
    return


@app.cell
def _(con, mo):
    mo.sql.conn = con
    return


@app.cell
def _():
    return


@app.cell
def _(mo):
    _df = mo.sql(
        f"""
        show tables
        """
    )
    return


@app.cell
def _(duckdb):
    df4 = duckdb.sql("SELECT * FROM 'Parquets/d_mda_state_INC_d20240729224455_f49f9b7c-8d51-4fde-a3d1-f0422220380d_0.parquet' LIMIT 1000").pl()
    return (df4,)


@app.cell
def _(df4, mo):
    mo.ui.table(df4)
    return


if __name__ == "__main__":
    app.run()
