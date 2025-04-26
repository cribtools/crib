# Crib &emsp; [![Latest Version]][crates.io] [![crib msrv]][Rust 1.88]

[Latest Version]: https://img.shields.io/crates/v/crib.svg
[crates.io]: https://crates.io/crates/crib
[crib msrv]: https://img.shields.io/crates/msrv/crib.svg?label=crib%20msrv&color=lightgray
[Rust 1.88]: https://releases.rs/docs/1.88.0/

**Crib** reads multiple bigWig files concurrently, whether local or remote (via http or S3).

Genomic track files come in many varieties. One of the most common binary file formats for quantitative measurements across the genome is [bigWig](https://genome.ucsc.edu/goldenpath/help/bigWig.html). **Crib** makes it possible to access a specific genomic region of multiple bigWig files concurrently and in bulk.

---
## CLI
`crib view` reads a single genomic region from multiple files, and outputs the results as a [bedGraph](https://genome.ucsc.edu/goldenpath/help/bedgraph.html) to `stdout` .

`crib view <chrom>:<start>-<end> <files>`

### Examples
- `crib view 3:40000-60000 s3://bucket/path/file.bw`
- `crib view 13:4000-245000 local/path/*.bw second.bw s3://bucket/path/another.bw`
- `crib view X:4000-245000 s3://bucket/path/ > output.bg` 
- `crib view 5:16000-18000 https://example.org/bw/file.bw > output.bg` 

## Library
The associated library's API is not stable. It is likely that it will change substantially.

## Limitations
1\. Currently, the library used for accessing BigWig files ([bigtools](https://crates.io/crates/bigtools)) breaks up large queries into smaller queries (blocks) suitable for caching (10 KB). It then queries using [attohttpc](https://crates.io/crates/attohttpc), which is sync. This means that:

- Async cooperative reading of multiple files is not possible with **bigtools**. Therefore **crib** spawns an OS thread for every file accessed concurrently. This shouldn't be a problem on a conventional computer, but it could impact throughput in resource-constrained environments.
- Querying large genomic regions means a large number of *get* requests will be made e.g. to your S3 instance. This might have cost or throttling implications.

2\. Querying is limited to a genomic range from a single chromosome or contig e.g. `3:15000-50000`.

<br>

#### License

<sup>
Licensed under either of <a href="LICENSE-APACHE">Apache License, Version 2.0</a> or <a href="LICENSE-MIT">MIT license</a> at your option.
</sup>
