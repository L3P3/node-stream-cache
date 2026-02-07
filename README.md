# node-stream-cache

A simple way to cache and replay readable streams.

## Features

- Cache and replay readable streams
- Pipe to multiple destinations simultaneously
- Backpressure handling for large streams
- Asynchronous replay prevents I/O blocking

## Usage

```js
var StreamCache = require('stream-cache');
var fs          = require('fs');

var cache = new StreamCache();
fs.createReadStream(__filename).pipe(cache);

// Cache can now be piped anywhere, even before the readable stream finishes.
cache.pipe(process.stdout);
```

## Backpressure Handling

When replaying cached data to a new destination after the stream has ended, the library automatically handles backpressure by:

- Splitting data into 64KB chunks
- Sending chunks asynchronously using `setImmediate`
- Respecting backpressure by listening to the `drain` event
- Preventing I/O blocking even with very large streams (100+ MB)

This ensures that piping large cached streams doesn't freeze your application.

```js
// Example: Cache a large file and replay it multiple times without freezing
var cache = new StreamCache();
fs.createReadStream('large-file.dat').pipe(cache);

// After the stream ends, you can pipe it to multiple destinations
// without blocking the event loop
cache.on('end', function() {
  cache.pipe(dest1);
  cache.pipe(dest2);
  cache.pipe(dest3);
  // Each destination receives data in chunks asynchronously
});
```
