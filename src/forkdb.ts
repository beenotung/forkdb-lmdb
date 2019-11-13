import {
  ExtendedTxn,
  Key,
  KeyType,
  newCursor,
  OpenedEnv,
} from 'typestub-node-lmdb';

type Fork = {
  parentID: number;
  childIDs: number[];
};

const forkIdToKey = (forkId: number) => 'fork' + ':' + forkId;

export function openForkDB(env: OpenedEnv) {
  const forkDB = env.openDbi({
    name: 'fork',
    create: true,
  });

  function loadForkRecord(txn: ExtendedTxn, forkId: number): Fork | null {
    const key = forkIdToKey(forkId);
    return txn.getObject<Fork>(forkDB, key);
  }

  function saveForkRecord(txn: ExtendedTxn, forkId: number, fork: Fork) {
    const key = forkIdToKey(forkId);
    txn.putObject(forkDB, key, fork);
  }

  function fork(parentID: number) {
    const txn = env.beginTxn();
    // allocate child ID
    let childID = parentID + 1;
    let child: Fork | null;
    for (
      child = loadForkRecord(txn, childID);
      child !== null;
      childID = (childID + 1) % (65536 - 10),
        child = loadForkRecord(txn, childID)
    ) {
      // try until this childID is not occupied
      if (childID === parentID) {
        console.error(`fork pool is full:`, { parentID, childID });
        throw new Error('fork pool is full');
      }
    }
    child = {
      parentID,
      childIDs: [],
    };
    const parent = loadForkRecord(txn, parentID);
    if (parent === null) {
      throw new Error(`fork parent '${parentID}' not found`);
    }
    parent.childIDs.push(childID);
    saveForkRecord(txn, childID, child);
    saveForkRecord(txn, parentID, parent);
    txn.commit();
    const dbi = env.openDbi({
      name: forkIdToKey(childID),
      create: true,
    });
    dbi.close();
    return childID;
  }

  function loadParent(txn: ExtendedTxn, childId: number) {
    if (childId === 0) {
      // this is root fork, don't have parent
      return null;
    }
    const child = loadForkRecord(txn, childId);
    if (child === null) {
      throw new Error(`fork record '${childId}' not found`);
    }
    const parentId = child.parentID;
    return loadFork(parentId);
  }

  function loadFork(forkId: number) {
    const dbi = env.openDbi({
      name: forkIdToKey(forkId),
    });
    const self = {
      forkId,
      dbi,
      fork() {
        const childId = fork(forkId);
        return loadFork(childId);
      },
      wrapTxn(txn: ExtendedTxn) {
        const parent = loadParent(txn, forkId)?.wrapTxn(txn);
        const self = {
          getString(key: Key, keyType?: KeyType): string | null {
            let value = txn.getString(dbi, key, keyType);
            if (value === null && parent) {
              value = parent.getString(key, keyType);
            }
            return value;
          },
          getBinary(key: Key, keyType?: KeyType): Buffer | null {
            let value = txn.getBinary(dbi, key, keyType);
            if (value === null && parent) {
              value = parent.getBinary(key, keyType);
            }
            return value;
          },
          getNumber(key: Key, keyType?: KeyType): number | null {
            let value = txn.getNumber(dbi, key, keyType);
            if (value === null && parent) {
              value = parent.getNumber(key, keyType);
            }
            return value;
          },
          getBoolean(key: Key, keyType?: KeyType): boolean | null {
            let value = txn.getBoolean(dbi, key, keyType);
            if (value === null && parent) {
              value = parent.getBoolean(key, keyType);
            }
            return value;
          },
          putString(key: Key, value: string, keyType?: KeyType): void {
            txn.putString(dbi, key, value, keyType);
          },
          putBinary(key: Key, value: Buffer, keyType?: KeyType): void {
            txn.putBinary(dbi, key, value, keyType);
          },
          putNumber(key: Key, value: number, keyType?: KeyType): void {
            txn.putNumber(dbi, key, value, keyType);
          },
          putBoolean(key: Key, value: boolean, keyType?: KeyType): void {
            txn.putBoolean(dbi, key, value, keyType);
          },
          del(key: Key, keyType?: KeyType): void {
            txn.del(dbi, key, keyType);
          },
          commit(): void {
            txn.commit();
          },
          abort(): void {
            txn.abort();
          },
          getObject<T>(key: Key, keyType?: KeyType): T | null {
            const value = self.getString(key, keyType);
            if (value === null) {
              return value;
            }
            return JSON.parse(value);
          },
          putObject(key: Key, value: any, keyType?: KeyType): void {
            self.putString(key, JSON.stringify(value), keyType);
          },
          // similar to 'use database' in mysql, change the implicit dbi
          changeFork(forkId: number) {
            // TODO
          },
        };
        return self;
      },
      beginTxn() {
        const txn = env.beginTxn();
        return self.wrapTxn(txn);
      },
      /**
       * if no child, delete directly;
       * if only one child, merge into child;
       * if multiple child, keep the fork, but mark for delete
       * */
      drop() {
        const txn = env.beginTxn();
        const fork = loadForkRecord(txn, forkId);
        if (fork === null) {
          if ('silent') {
            txn.abort();
            return;
          }
          throw new Error(`fork record '${forkId}' not found`);
        }
        // if no child, delete directly;
        if (fork.childIDs.length === 0) {
          dbi.drop();
          txn.commit();
          return;
        }
        // if only one child, merge into child;
        if (fork.childIDs.length === 1) {
          // TODO
        }
        // if multiple child, keep the fork, but mark for delete
        // TODO
        txn.abort();
      },
    };
    return self;
  }

  function loadRoot(forkId: number = 0) {
    const dbi = env.openDbi({
      name: forkIdToKey(forkId),
      create: true,
    });
    dbi.close();
    const txn = env.beginTxn();
    // auto create fork record if not exist
    let fork = loadForkRecord(txn, forkId);
    if (fork === null) {
      fork = {
        parentID: 0,
        childIDs: [],
      };
      saveForkRecord(txn, forkId, fork);
    }
    txn.commit();
    return loadFork(forkId);
  }

  function clearAll() {
    let txn = env.beginTxn();
    const cursor = newCursor(txn, forkDB);
    const forkKeys: string[] = [];
    for (let key = cursor.goToFirst(); key !== null; key = cursor.goToNext()) {
      forkKeys.push(key.toString());
    }
    txn.commit();
    txn = env.beginTxn();
    for (const forkKey of forkKeys) {
      txn.del(forkDB, forkKey);
    }
    txn.commit();
    for (const forkKey of forkKeys) {
      const dbi = env.openDbi({
        name: forkKey,
        create: true,
      });
      dbi.drop();
    }
  }

  return {
    fork,
    loadFork,
    loadRoot,
    clearAll,
  };
}
