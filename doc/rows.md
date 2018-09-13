# Row Based Access

> *v3 originally completely killed row-based access, however that also killed some use cases associated with the ease of use.*

> *Comparing to v2, v3 doesn't expand/contract rows added to the `Table` because most of the use cases with row based access is around processing data before it's written to parquet. v3 however agressively validates rows added to the tables before accepting those changes.*