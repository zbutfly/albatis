# Albatis Change List

## 1.6.0-SNAPSHOT

### core

- Class `Qualifier` for `String` name of table/field, include family, prefix, and to be extended.

### hbase-io

- HBase streaming input
  - Enable sub-table by family and prefix.
  - Enable parallel scanning for one table based on regions split.
    - Move origin sub-table splitting from `SubjectFormat` into `HbaseInput`.
  - Enable skip based on rowkey, region number or rows count.
- Configurations:
  - Max columns (cells) per row limitation
  - Skip mode
  - Sub-table mode
- Miscellaneous:
  - Utils classes package separate from `Hbases`