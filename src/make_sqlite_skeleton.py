"""
Defines function make_sqlite_skeleton()
"""

import sqlite3
from collections import defaultdict


def make_sqlite_skeleton(col_pairs: list[tuple], output_db_path: str) -> None:
    """Creates a SQLite database containing empty tables which
    obey the specified database schema. This enables the use of any
    SQLite schema visualisation tool for visualising the schema
    (I like dbvisualizer)

    Example:
        >>> make_sqlite_skeleton(
        ...         col_pairs=[
        ...             ( ("users_tbl","id"), ("transactions_tbl","user_id") ),
        ...             ( ("transactions_tbl","product_id"), ("products_tbl","id") ),
        ...             ( ("user_address_tbl","user_id"), ("users_tbl","id") ),
        ...         ],
        ...         output_db_path="./key_relationships_sqlite_skeleton.db",
        ...     )
        CREATE TABLE users_tbl( id , FOREIGN KEY (id) REFERENCES transactions_tbl(user_id) );
        CREATE TABLE transactions_tbl( user_id, product_id , FOREIGN KEY (product_id) REFERENCES products_tbl(id) );
        CREATE TABLE products_tbl( id );
        CREATE TABLE user_address_tbl( user_id , FOREIGN KEY (user_id) REFERENCES users_tbl(id) );
    """
    sql_con = sqlite3.connect(output_db_path)
    sql_cur = sql_con.cursor()

    tbl_col_ref = defaultdict(set)
    for col1, col2 in col_pairs:
        tbl_col_ref[col1[0]].add(col1[1])
        tbl_col_ref[col2[0]].add(col2[1])

    for tbl_name, tbl_cols in tbl_col_ref.items():
        create_tbl_statement = (
            f'CREATE TABLE {tbl_name}( {", ".join([str(c) for c in tbl_cols])}'
        )
        for col1, col2 in col_pairs:
            if col1[0] == tbl_name:
                create_tbl_statement += (
                    f" , FOREIGN KEY ({col1[1]}) REFERENCES {col2[0]}({col2[1]})"
                )
        create_tbl_statement += " );"
        print(create_tbl_statement)
        sql_cur.execute(create_tbl_statement)

    sql_con.close()
