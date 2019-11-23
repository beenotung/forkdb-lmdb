import { expect } from 'chai';
import 'mocha';
import { newEnv } from 'typestub-node-lmdb';
import { openForkDB } from '../src/forkdb';

// tslint:disable:no-unused-expression

describe('forkdb TestSuit', () => {
  const env = newEnv().open({
    path: 'data',
    maxDbs: 20,
  });
  const { clearAll, loadRoot, dropFork, loadFork } = openForkDB(env);

  const WhoAmI = 'WhoAmI';

  function expectForkNotExist(forkId: number) {
    expect(loadFork.bind(null, forkId)).to.throw('MDB_NOTFOUND: No matching key/data pair found');
  }

  it('should load root without error', () => {
    loadRoot();
  });

  it('should fork without error', () => {
    const root = loadRoot();
    const child = root.fork();
    expect(root.forkId).not.equals(child.forkId);
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

    const child = root.fork();
    txn = child.beginTxn();
    expect(txn.getString('foo')).equals('bar');
    txn.commit();
  });

  it('should allow child get value from grandparent if not updated', () => {
    const root = loadRoot();
    let txn = root.beginTxn();
    txn.putString('foo', 'bar');
    txn.commit();

    const child = root.fork();

    const grandchild = child.fork();
    txn = grandchild.beginTxn();
    expect(txn.getString('foo')).equals('bar');
    txn.commit();
  });

  it('should allow child get value from itself if updated', () => {
    const root = loadRoot();
    let txn = root.beginTxn();
    txn.putString('foo', 'bar');
    txn.commit();

    const child = root.fork();
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

    const child = root.fork();
    txn = child.beginTxn();
    txn.putString('foo', 'bar2');
    txn.commit();

    txn = root.beginTxn();
    expect(txn.getString('foo')).equals('bar');
    txn.commit();
  });

  it('should change between forks within the single transaction', () => {
    const root = loadRoot();
    const child = root.fork();
    {
      const txn = root.beginTxn();
      txn.putString(WhoAmI, 'root');
      txn.changeFork(child.forkId);
      txn.putString(WhoAmI, 'child');
      txn.commit();
    }
    {
      const txn = root.beginTxn({ readOnly: true });
      expect(txn.getString(WhoAmI)).equals('root');
      txn.changeFork(child.forkId);
      expect(txn.getString(WhoAmI)).equals('child');
      txn.commit();
    }
  });

  it('should be able to fork after transaction has started', () => {
    const root = loadRoot();
    const txn = root.beginTxn();
    txn.putString(WhoAmI, 'root');
    const child = txn.fork();
    txn.putString(WhoAmI, 'still root');
    txn.changeFork(child);
    txn.putString(WhoAmI, 'child');
    txn.commit();

    {
      const txn = root.beginTxn({ readOnly: true });
      expect(txn.getString(WhoAmI)).equals('still root');
      txn.commit();
    }
    {
      const txn = child.beginTxn({ readOnly: true });
      expect(txn.getString(WhoAmI)).equals('child');
      txn.commit();
    }
  });

  it('should drop non-existing fork without error', () => {
    // get an available forkId
    let fork = loadRoot().fork();

    // drop this fork
    fork.drop();

    // test if it pass without error
    dropFork(fork.forkId);
  });

  it('should clear all forks without error', () => {
    clearAll();
  }).timeout(5 * 1000);

  it('should load non-existing fork with error', () => {
    expectForkNotExist(987);
  });

  it('should drop fork without child', function() {
    const parent = loadRoot().fork();
    const child = parent.fork();

    // set values to distinct parent and child
    {
      const txn = env.beginTxn();
      txn.putString(parent.dbi, WhoAmI, 'parent');
      txn.putString(child.dbi, WhoAmI, 'child');
      txn.commit();
    }

    // drop child
    expect(child.drop()).equals('ok');
    expectForkNotExist(child.forkId);

    // test if parent is affected
    {
      const txn = env.beginTxn();
      expect(txn.getString(parent.dbi, WhoAmI)).equals('parent');
      txn.commit();
    }
  });

  it('should drop fork with one child', function() {
    const parent = loadRoot().fork();
    const child = parent.fork();
    const grandchild = child.fork();

    // set value to distinct forks
    {
      const txn = env.beginTxn();
      txn.putString(parent.dbi, WhoAmI, 'parent');
      txn.putString(child.dbi, WhoAmI, 'child');
      txn.putString(grandchild.dbi, WhoAmI, 'grandchild');
      txn.commit();
    }

    // drop child
    expect(child.drop()).equals('ok');
    expectForkNotExist(child.forkId);

    // test if other forks are affected
    {
      const txn = env.beginTxn();
      expect(txn.getString(parent.dbi, WhoAmI)).equals('parent');
      expect(txn.getString(grandchild.dbi, WhoAmI)).equals('grandchild');
      txn.commit();
    }
  });

  it('should drop fork with multiple children when possible', function() {
    const parent = loadRoot().fork();
    const child = parent.fork();
    const grandchild1 = child.fork();
    const grandchild2 = child.fork();

    // set value to distinct forks
    {
      const txn = env.beginTxn();
      txn.putString(parent.dbi, WhoAmI, 'parent');
      txn.putString(child.dbi, WhoAmI, 'child');
      txn.putString(grandchild1.dbi, WhoAmI, 'grandchild1');
      txn.putString(grandchild2.dbi, WhoAmI, 'grandchild2');
      txn.commit();
    }

    // drop child
    // expect(child.drop()).equals('ok');
    // expectForkNotExist(child.forkId);
    child.drop();

    // test if other forks are affected
    {
      const txn = env.beginTxn();
      expect(txn.getString(parent.dbi, WhoAmI)).equals('parent');
      expect(txn.getString(grandchild1.dbi, WhoAmI)).equals('grandchild1');
      expect(txn.getString(grandchild2.dbi, WhoAmI)).equals('grandchild2');
      txn.commit();
    }
  });

});
// tslint:enable:no-unused-expression
