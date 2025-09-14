from pyspark.sql import DataFrame, functions as F


def build_product_category_pairs(
        products: DataFrame,
        categories: DataFrame,
        product_categories: DataFrame,
        product_id_col: str = "product_id",
        product_name_col: str = "product_name",
        category_id_col: str = "category_id",
        category_name_col: str = "category_name",
) -> DataFrame:
    """
    Return a single DataFrame with columns:
      - product_name
      - category_name (nullable)
    It lists all (Product Name, Category Name) pairs and includes one row per product
    without categories where category_name is null.
    """
    pc = product_categories.dropDuplicates([product_id_col, category_id_col]).alias("pc")
    p = products.alias("p")
    c = categories.alias("c")

    joined = (
        p.join(
            pc,
            F.col(f"p.{product_id_col}") == F.col(f"pc.{product_id_col}"),
            how="left",
        )
        .join(
            c,
            F.col(f"pc.{category_id_col}") == F.col(f"c.{category_id_col}"),
            how="left",
        )
        .select(
            F.col(f"p.{product_name_col}").alias("product_name"),
            F.col(f"c.{category_name_col}").alias("category_name"),
        )
    )

    return joined
