# Financial Transactions Analysis (SQL Project)

## Project Description

This project simulates a financial transactions system with support for:

- transaction processing
- balance updates
- analytical reporting

The main goal is to demonstrate SQL skills in data processing, aggregation, and performance optimization.

---

## Key Features

### Transaction Processing
- balance updates using transactions
- protection from race conditions (`SELECT ... FOR UPDATE`)

### Analytical Reporting
- reports by country and operation type
- support for custom date ranges
- hierarchical totals using `WITH ROLLUP`

### Performance Optimization
- indexes for frequent queries
- pre-aggregated table for faster reporting
- separation of raw and aggregated data

### Efficient Date Range Handling
- split into:
- first partial day
- full days (aggregated)
- last partial day
- correct boundary handling using `LEAST` and `GREATEST`

## Advanced features:
- partitioning
- scheduled aggregation
- partition rotation

### Data Partitioning

To improve performance for large datasets, the `user_operations` table is partitioned by date.

This allows:
- faster filtering by date range
- efficient data retention (dropping old partitions)
- improved query performance on large volumes

Partition rotation is implemented via a scheduled procedure that keeps only recent data.

---

## Data Model

Core tables:
- users
- user_operations
- countries
- currencies
- operation_types

## Technologies

- SQL (MySQL 8)
- Stored Procedures
- Indexing
- Aggregation
- Partitioning

## Example Use Case

Generate report for a custom period:

```sql
CALL GetReportByDate('2026-03-17 20:00:00', '2026-03-20 10:00:00');