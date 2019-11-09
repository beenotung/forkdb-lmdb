# forkdb-lmdb

Object-prototype-like abstraction over lmdb.

[![npm Package Version](https://img.shields.io/npm/v/forkdb-lmdb.svg?maxAge=2592000)](https://www.npmjs.com/package/forkdb-lmdb)

The state is forkable / extensible in the sense similar to `Object.create()` in Javascript.

The values are indexed by key.
When a value is being looked up,
we first check if it's present in the current (child) state,
if not, we'll lookup it in the parent state recursively.

Examples refers to [forkdb.spec.ts](test/forkdb.spec.ts)
