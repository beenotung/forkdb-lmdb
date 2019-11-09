import { expect } from 'chai';
import 'mocha';
import { newEnv } from 'typestub-node-lmdb';
import { openForkDB } from '../src/forkdb';

// tslint:disable:no-unused-expression

describe('forkdb TestSuit', () => {
  const env = newEnv().open({
    path: 'data',
    maxDbs: 10,
  });
  const { clearAll, loadRoot } = openForkDB(env);

  it('should load root without error', () => {
    loadRoot();
  });

  it('should fork without error', () => {
    const root = loadRoot();
    const child = root.fork(1);
    console.log('forked into:', child.forkId);
  });

  it('should be able to store and get back value', () => {
    const root = loadRoot();
    const txn = root.beginTxn();
    const key = 'num';
    const oldValue = txn.getNumber(key) || 0;
    const newValue = oldValue + 1;
    txn.putNumber(key, newValue);
    expect(txn.getNumber(key)).equals(newValue);
    txn.commit();
  });

  it('should allow child get value from parent if not updated', () => {
    const root = loadRoot();
    let txn = root.beginTxn();
    txn.putString('foo', 'bar');
    txn.commit();

    const child = root.fork(1);
    txn = child.beginTxn();
    expect(txn.getString('foo')).equals('bar');
    txn.commit();
  });

  it('should allow child get value from itself if updated', () => {
    const root = loadRoot();
    let txn = root.beginTxn();
    txn.putString('foo', 'bar');
    txn.commit();

    const child = root.fork(1);
    txn = child.beginTxn();
    txn.putString('foo', 'bar2');
    expect(txn.getString('foo')).equals('bar2');
    txn.commit();
  });

  it('should protect parent state from modification by child', () => {
    const root = loadRoot();
    let txn = root.beginTxn();
    txn.putString('foo', 'bar');
    txn.commit();

    const child = root.fork(1);
    txn = child.beginTxn();
    txn.putString('foo', 'bar2');
    txn.commit();

    txn = root.beginTxn();
    expect(txn.getString('foo')).equals('bar');
    txn.commit();
  });

  it('should clear all forks without error', () => {
    clearAll();
  });
});
// tslint:enable:no-unused-expression