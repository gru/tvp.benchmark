|           Method | TestDataRowCount | TestParameterRowCount | TestParameterCount |     Mean |     Error |    StdDev |
|----------------- |----------------- |---------------------- |------------------- |---------:|----------:|----------:|
|      StringSplit |             1000 |                     3 |                100 | 1.382 ms | 0.0304 ms | 0.0897 ms |
|        SimpleUDT |             1000 |                     3 |                100 | 2.202 ms | 0.0429 ms | 0.0628 ms |
|      SimplePKUDT |             1000 |                     3 |                100 | 2.498 ms | 0.0336 ms | 0.0314 ms |
|     MemoryOptUDT |             1000 |                     3 |                100 | 2.496 ms | 0.0476 ms | 0.0510 ms |
| MemoryOptHashUDT |             1000 |                     3 |                100 | 2.053 ms | 0.0397 ms | 0.0582 ms |
