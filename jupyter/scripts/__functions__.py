from IPython.display import clear_output
from sqlalchemy import create_engine
from urllib.parse import quote_plus


# ---- DATA OPS
def get_dump_path(view_name):
    return f"spark/dump/{mynameis}/{view_name}"


def make_storage_path(path):
    path = path.strip('/')
    return f"s3a://{path}"


def select(query, spark_view_name=None, lowercase=True, connection=None):
    if not connection:
        connection = "fc test"

    if connection == "fc test":
        ip, port, sid, username, password = connector.get_fc_creds(env=env, master_password=master_password)
    else:
        raise ValueError(f"Invalid connection: {connection}")

    if spark_view_name:
        jdbcDriver = "oracle.jdbc.driver.OracleDriver"
        url = f"jdbc:oracle:thin:{username}/{password}@{ip}:{port}/{sid}"
        jdbcDF = spark.read.format("jdbc") \
            .option("url", url) \
            .option("batchsize", "300000") \
            .option("fetchsize", "300000") \
            .option("query", query) \
            .option("driver", jdbcDriver) \
            .load()

        jdbcDF.createOrReplaceTempView(spark_view_name)
        return jdbcDF
    else:
        with connection.cursor() as cursor:
            cursor.execute(query)
            data = cursor.fetchall()
            if lowercase:
                columns = [x[0].lower() for x in cursor.description]
            else:
                columns = [x[0] for x in cursor.description]
            df = pd.DataFrame(data, columns=columns)
        return df


def select_dump_and_read(query, spark_view_name):
    df = select(query=query, spark_view_name=spark_view_name)

    path = make_storage_path(get_dump_path(spark_view_name))

    df.write.orc(path, mode="overwrite")
    df = spark.read.orc(path)

    df.createOrReplaceTempView(spark_view_name)
    return df


def show(query, clear=True, n=10):
    if ' ' not in query:
        query = f"select * from {query}"
    df = spark.sql(query).limit(n).toPandas()
    if clear:
        clear_output()
    return df


def to_pandas(query, clear=True):
    df = spark.sql(query).toPandas()
    if clear:
        clear_output()
    return df


def sql(query, clear=True):
    df = spark.sql(query)
    if clear:
        clear_output()
    return df


# CONTINUE
def cache(query, spark_view_name, path=None, overwrite=True):
    if not path:
        path = get_dump_path(spark_view_name)
    else:
        path = os.path.join(path, spark_view_name)

    path = make_storage_path(path)

    if not overwrite:
        try:
            df = spark.read.orc(path)
        except:
            overwrite = True

    if overwrite:
        df = sql(query=query)

        df.write.orc(path, mode="overwrite")
        df = spark.read.orc(path)

    df.createOrReplaceTempView(spark_view_name)
    return df


def plot_pie(labels, sizes, title, autopct=None, **kwargs):
    if autopct:
        wedges, texts, _ = plt.pie(sizes, wedgeprops=dict(width=0.5), startangle=90, autopct=autopct, **kwargs)
    else:
        wedges, texts = plt.pie(sizes, wedgeprops=dict(width=0.5), startangle=90, **kwargs)
    _ = plt.title(title, pad=30, fontsize=15)

    bbox_props = dict(boxstyle="square,pad=0.4", fc="w", ec="k", lw=0.72)
    kw = dict(arrowprops=dict(arrowstyle="-"),
              bbox=bbox_props, zorder=0, va="center")

    ax = plt.gca()

    for i, p in enumerate(wedges):
        ang = (p.theta2 - p.theta1) / 5 + p.theta1
        y = np.sin(np.deg2rad(ang))
        x = np.cos(np.deg2rad(ang))
        horizontalalignment = {-1: "right", 1: "left"}[int(np.sign(x))]
        connectionstyle = "angle,angleA=0,angleB={}".format(ang)
        kw["arrowprops"].update({"connectionstyle": connectionstyle})
        ax.annotate(labels[i], xy=(x, y), xytext=(1. * np.sign(x), 1.4 * y),
                    horizontalalignment=horizontalalignment, **kw)

    plt.show()


def load_table_from_hadoop(scheme, table_name=None, view_name=None):
    if not table_name:
        path = get_dump_path(scheme)
    else:
        path = os.path.join(scheme, table_name)

    path = make_storage_path(path)

    if not view_name:
        view_name = table_name

    df = spark.read.orc(path)

    if view_name:
        df.createOrReplaceTempView(view_name)

    return df


def load_raw_table_from_hadoop(scheme, table_name, view_name=None, data_schema=None):
    if not view_name:
        view_name = table_name

    path = make_storage_path(os.path.join(scheme, table_name))
    df = spark.read.orc(path)

    if data_schema:
        columns = [F.col(field.name).astype(field.dataType) for field in data_schema.fields]
    else:
        columns = [F.col(field.name).astype(T.StringType()) for field in df.schema.fields]

    df = df.select(columns)

    if view_name:
        df.createOrReplaceTempView(view_name)

    return df


# def upload_to_postgres(df, host, port, dbname, schema, table_name):
#     username = input('Input DB username: ')
#     password = input('Input DB password: ')
#     engine = create_engine(f'postgresql://{username}:{quote_plus(password)}@{host}:{port}/{dbname}')
#     df.to_sql(table_name, engine, schema=schema, if_exists='append', index=False)
#     engine.dispose()

# ----- VISUALIZATION
def adaptive_format_d(d):
    if d < 1_000:
        return d
    elif 1_000 <= d < 1_000_000:
        return f"{d / 1_000:.1f} тыс."
    elif 1_000_000 <= d < 1_000_000_000:
        return f"{d / 1_000_000:.1f} млн."
    elif 1_000_000_000 <= d < 1_000_000_000_000:
        return f"{d / 1_000_000_000:.1f} млрд."
    elif 1_000_000_000_000 <= d < 1_000_000_000_000_000:
        return f"{d / 1_000_000_000_000:.1f} трлн."
    else:
        return "inf"


def plot_comparsion_line(x1, y1, label1, x2, y2, label2, title, xlabel=None, ylabel=None, num_y_ticks=8,
                         xticks_rotation=45):
    plt.plot(x1, y1, label=label1)
    plt.plot(x2, y2, label=label2)

    min_y = min(min(y1), min(y2))
    max_y = max(max(y1), max(y2))
    y_ticks = np.arange(min_y, max_y, (max_y - min_y) // num_y_ticks)
    y_labels = [adaptive_format_d(y) for y in y_ticks]

    if xticks_rotation is not None:
        plt.xticks(rotation=xticks_rotation)
    plt.yticks(y_ticks, y_labels)

    plt.legend(*plt.gca().get_legend_handles_labels())
    plt.title(title, pad=20)

    if xlabel:
        plt.xlabel(xlabel, labelpad=15)
    if ylabel:
        plt.ylabel(ylabel, labelpad=10)


def plot_XY(x, y, label, title=None, xlabel=None, ylabel=None, num_y_ticks=8, xticks_rotation=None):
    plt.plot(x, y, label=label)

    min_y = min(y)
    max_y = max(y)

    if xticks_rotation is not None:
        plt.xticks(rotation=xticks_rotation)

    if num_y_ticks:
        y_ticks = np.arange(min_y, max_y, (max_y - min_y) // num_y_ticks)
        y_labels = [adaptive_format_d(y) for y in y_ticks]

        plt.yticks(y_ticks, y_labels)

    plt.legend(*plt.gca().get_legend_handles_labels())

    if title:
        plt.title(title, pad=20)

    if xlabel:
        plt.xlabel(xlabel, labelpad=15)
    if ylabel:
        plt.ylabel(ylabel, labelpad=10)


