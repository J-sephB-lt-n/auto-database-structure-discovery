from src.make_sqlite_skeleton import make_sqlite_skeleton

make_sqlite_skeleton(
    col_pairs=[
        (("bank_account_prod", "account_id"), ("account_prod", "account_id")),
        (("bank_account_prod", "bank_account_id"), ("bank_account_transaction_prod", "bank_account_id")),
        (("bank_account_prod", "account_id"), ("deposit_prod", "account_id")),
        (("bank_account_prod", "account_id"), ("hub_prod", "account_id")),
        (("bank_account_prod", "account_id"), ("transaction_prod", "account_id")),
        (("account_prod", "account_id"), ("bank_account_transaction_prod", "account_id")),
        (("account_prod", "account_id"), ("deposit_prod", "account_id")),
    ],
    output_db_path="example.db",
)
