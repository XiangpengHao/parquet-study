
## Compressed vs uncompressed
You always want to compress your data. (unless you know what you are doing, e.g., enabled filter pushdown).

If your data is on object storage, use zstd; otherwise, use LZ4.

Why?
Uncompressed data is large, they cost a lot of I/O. You decompressor/decoder throughput is much faster than your I/O throughput.


How to shrink size without using general purpose compression?

## Page stats and location index
You always want to enable them. They cost very little space, but is incredibly useful for certain queries.


## Bloom filter
You only want to enable bloom filter for string columns.

You often don't want to enable bloom filter for columns whose average length is less than 10 bytes.

## Dictionary size
If your column's ndv ratio is less than 50%, you should make sure the dictionary size is large enough to fit the whole column.

If dictionary size is too small, some values are plain encoded, which is bad.

## Row group size
A file should contain more than 4 row groups to allow row-group level parallelism.

A row group size should be less than 64MB, because query engine will read the whole row group into memory.

## File size

## Use a StringView or StringDictionary

Without string view: 4.2s, with string view: 2.8s.

## Compiler flags:

```
env RUSTFLAGS='-C target-cpu=native'
```
This tells the compiler to use the native CPU instructions, resulting in less portable binary but usually faster.
