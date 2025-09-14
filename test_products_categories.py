import pytest
from pyspark.sql import SparkSession
from products_categories import build_product_category_pairs


@pytest.fixture(scope="session")
def spark():
    spark = SparkSession.builder.master("local[1]").appName("pc-tests").getOrCreate()
    return spark


def to_set(df):
    return set(map(tuple, df.collect()))


def test_basic_pairs_and_uncategorized(spark):
    products = spark.createDataFrame(
        [
            (1, "Milk"),
            (2, "Bread"),
            (3, "Eggs"),
            (4, "Apples"),
            (5, "Oranges"),
        ],
        ["product_id", "product_name"],
    )

    categories = spark.createDataFrame(
        [
            (10, "Dairy"),
            (20, "Bakery"),
            (30, "Produce"),
        ],
        ["category_id", "category_name"],
    )

    product_categories = spark.createDataFrame(
        [
            (1, 10),  # Milk -> Dairy
            (2, 20),  # Bread -> Bakery
            (4, 30),  # Apples -> Produce
            (4, 30),  # duplicate mapping to test dedup
        ],
        ["product_id", "category_id"],
    )

    out = build_product_category_pairs(products, categories, product_categories)

    expected = {
        ("Milk", "Dairy"),
        ("Bread", "Bakery"),
        ("Apples", "Produce"),
        ("Eggs", None),
        ("Oranges", None),
    }
    assert to_set(out.select("product_name", "category_name")) == expected


def test_many_to_many_and_custom_columns(spark):
    products = spark.createDataFrame(
        [
            (1, "CPU"),
            (2, "GPU"),
        ],
        ["pid", "pname"],
    )

    categories = spark.createDataFrame(
        [
            (100, "Compute"),
            (200, "Graphics"),
        ],
        ["cid", "cname"],
    )

    product_categories = spark.createDataFrame(
        [
            (1, 100),  # CPU -> Compute
            (1, 200),  # CPU -> Graphics
            (2, 200),  # GPU -> Graphics
        ],
        ["pid", "cid"],
    )

    out = build_product_category_pairs(
        products,
        categories,
        product_categories,
        product_id_col="pid",
        product_name_col="pname",
        category_id_col="cid",
        category_name_col="cname",
    )

    expected = {
        ("CPU", "Compute"),
        ("CPU", "Graphics"),
        ("GPU", "Graphics"),
    }
    assert to_set(out.select("product_name", "category_name")) == expected


def test_no_categories_at_all(spark):
    products = spark.createDataFrame(
        [(1, "A"), (2, "B")],
        ["product_id", "product_name"],
    )
    categories = spark.createDataFrame([], "category_id INT, category_name STRING")
    product_categories = spark.createDataFrame([], "product_id INT, category_id INT")

    out = build_product_category_pairs(products, categories, product_categories)
    expected = {("A", None), ("B", None)}
    assert to_set(out.select("product_name", "category_name")) == expected
