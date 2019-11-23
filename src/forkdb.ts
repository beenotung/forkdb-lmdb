import {
  Dbi,
  ExtendedReadonlyTxn,
  ExtendedTxn,
  Key,
  KeyType,
  newCursor,
  OpenedEnv,
  ReadonlyTxn,
  Txn,
} from 'typestub-node-lmdb';

type ForkRecord = {
  parentID: number;
  childIDs: number[];
};

const forkIdToKey = (forkId: number) => 'fork' + ':' + forkId;

function isReadonly(txn: ExtendedReadonlyTxn | ExtendedTxn): boolean {
  return !(txn as ExtendedTxn).putObject;
}

export type ReadonlyForkTxn = {
  txn: ExtendedReadonlyTxn;
  parent: Fork | null;
  getString(key: Key, keyType?: KeyType): string | null;
  getBinary(key: Key, keyType?: KeyType): Buffer | null;
  getNumber(key: Key, keyType?: KeyType): number | null;
  getBoolean(key: Key, keyType?: KeyType): boolean | null;
  commit(): void;
  abort(): void;
  getObject<T>(key: Key, keyType?: KeyType): T | null;
  // similar to 'use database' in mysql, change the implicit dbi
  changeFork(forkId_or_fork: number | { forkId: number; dbi: Dbi }): void;
  forEachKey(f: (key: Key) => void): void;
};
export type DropResult = 'not_exist' | 'ok';
export type ForkTxn = ReadonlyForkTxn & {
  txn: ExtendedTxn;
  // cannot inline the impl, otherwise will mark the `fork` under `beginTxn` also deprecated
  fork: () => Fork;
  /**
   * if no child, delete directly;
   * if only one child, merge into child;
   * if multiple child, keep the fork, but mark for delete
   * */
  drop: () => DropResult;
  putString(key: Key, value: string, keyType?: KeyType): void;
  putBinary(key: Key, value: Buffer, keyType?: KeyType): void;
  putNumber(key: Key, value: number, keyType?: KeyType): void;
  putBoolean(key: Key, value: boolean, keyType?: KeyType): void;
  putObject(key: Key, value: any, keyType?: KeyType): void;
  del(key: Key, keyType?: KeyType): void;
};

interface IWrapTxn {
  (txn: ExtendedTxn): ForkTxn;

  (txn: ExtendedReadonlyTxn): ReadonlyForkTxn;
}

interface IBeginTxn {
  (options?: { readOnly?: false }): ForkTxn;

  (options: { readOnly: true }): ReadonlyForkTxn;
}

export type Fork = {
  forkId: number;
  dbi: Dbi;
  wrapTxn: IWrapTxn;
  beginTxn: IBeginTxn;
  /**
   * @deprecated cannot run within other transaction
   * use the `fork` under `self.beginTxn` or `self.wrapTxn` instead
   * */
  fork: () => Fork;
  /**
   * @deprecated cannot run within other transaction
   * use the `drop` under `self.beginTxn` or `self.wrapTxn` instead
   * */
  drop(): DropResult;
};

export function openForkDB(env: OpenedEnv) {
  const forkDB = env.openDbi({
    name: 'fork',
    create: true,
  });

  function loadForkRecord(
    txn: ExtendedReadonlyTxn,
    forkId: number,
  ): ForkRecord | null {
    const key = forkIdToKey(forkId);
    return txn.getObject<ForkRecord>(forkDB, key);
  }

  function saveForkRecord(txn: ExtendedTxn, forkId: number, fork: ForkRecord) {
    const key = forkIdToKey(forkId);
    txn.putObject(forkDB, key, fork);
  }

  function delForkRecord(txn: Txn, forkId: number) {
    const key = forkIdToKey(forkId);
    txn.del(forkDB, key);
  }

  function fork(txn: ExtendedTxn, parentID: number) {
    // allocate child ID
    let childID = parentID + 1;
    let child: ForkRecord | null;
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
    const dbi = env.openDbi({
      name: forkIdToKey(childID),
      create: true,
      txn,
    });
    // FIXME only reset+renew for readonly txn
    if (isReadonly(txn)) {
      txn.reset();
      txn.renew();
    }
    return { childID, dbi };
  }

  function loadParent(txn: ExtendedReadonlyTxn, childId: number): Fork | null {
    if (childId === 0) {
      // this is root fork, don't have parent
      return null;
    }
    const child = loadForkRecord(txn, childId);
    if (child === null) {
      throw new Error(`fork record '${childId}' not found`);
    }
    const parentId = child.parentID;
    return loadFork(parentId, (txn as ReadonlyTxn) as Txn);
  }

  function loadForkDbi(forkId: number, txn?: Txn) {
    return env.openDbi({ name: forkIdToKey(forkId), txn });
  }

  function migrateFork(args: {
    txn: ExtendedTxn;
    selfForkId: number;
    selfForkRecord?: ForkRecord;
    selfDbi?: Dbi;
    selfKeys?: string[];
    selfCost?: number;
    childForkId: number;
    childForkRecord?: ForkRecord;
    childDbi?: Dbi;
    childKeys?: string[];
    childCost?: number;
  }): 'self_to_child' | 'child_to_self' {
    const { txn, selfForkId, childForkId } = args;

    const selfForkRecord =
      args.selfForkRecord || loadForkRecord(txn, selfForkId);
    if (selfForkRecord === null) {
      console.error(`selfForkRecord not found, forkId:`, selfForkId);
      throw new Error('forkRecord not found');
    }
    const childForkRecord =
      args.childForkRecord || loadForkRecord(txn, childForkId);
    if (childForkRecord === null) {
      console.error(`childForkRecord not found, forkId:`, childForkId);
      throw new Error('forkRecord not found');
    }

    const selfDbi = args.selfDbi || loadForkDbi(selfForkId, txn);
    const childDbi = args.childDbi || loadForkDbi(childForkId, txn);

    const selfKeys = args.selfKeys || txn.keys(selfDbi);
    const childKeys = args.childKeys || txn.keys(childDbi);

    // cost if copy from 'child' to 'self'
    const selfCost =
      args.selfCost || childKeys.length + childForkRecord.childIDs.length;
    // cost if copy from 'self' to 'child'
    const childCost = args.childCost || selfKeys.length + 1;

    if (selfCost > childCost) {
      // copy from 'child' to 'self'
      const selfKeySet = new Set(selfKeys);
      childKeys.forEach(key => {
        const value = txn.getBinary(childDbi, key);
        if (value === null) {
          if (selfKeySet.has(key)) {
            txn.del(selfDbi, key);
          }
        } else {
          txn.putBinary(selfDbi, key, value);
        }
      });
      // update the parent ref on the grandchildren to self
      for (const grandChildForkId of childForkRecord.childIDs) {
        const grandChildForkRecord = loadForkRecord(txn, grandChildForkId);
        if (grandChildForkRecord === null) {
          console.error(
            `grandChildForkRecord not found, forkId:`,
            grandChildForkId,
          );
          throw new Error('forkRecord not found');
        }
        grandChildForkRecord.parentID = selfForkId;
        saveForkRecord(txn, grandChildForkId, grandChildForkRecord);
      }
      // delete child
      childDbi.drop({ txn });
      return 'child_to_self';
    } else {
      // copy from 'self' to 'child'
      const childKeySet = new Set(childKeys);
      selfKeys.forEach(key => {
        if (childKeySet.has(key)) {
          // child override self value
          return;
        }
        const value = txn.getBinary(selfDbi, key);
        if (value === null) {
          if (childKeySet.has(key)) {
            txn.del(childDbi, key);
          }
        } else {
          txn.putBinary(childDbi, key, value);
        }
      });
      // update the child ref on the parent to child
      const parentForkId = selfForkRecord.parentID;
      if (parentForkId !== 0) {
        // it really has parent
        const parentForkRecord = loadForkRecord(txn, parentForkId);
        if (parentForkRecord === null) {
          console.error(`parentForkRecord not found, forkId:`, parentForkId);
          throw new Error('forkRecord not found');
        }
        const { childIDs } = parentForkRecord;
        for (let i = 0; i < childIDs.length; i++) {
          if (childIDs[i] === selfForkId) {
            childIDs[i] = childForkId;
          }
        }
        saveForkRecord(txn, parentForkId, parentForkRecord);
      }
      // delete self
      selfDbi.drop({ txn });
      return 'self_to_child';
    }
  }

  function loadFork(forkId: number, dbi_or_txn?: Dbi | Txn): Fork {
    const dbi: Dbi = ((): Dbi => {
      if (!dbi_or_txn) {
        return loadForkDbi(forkId);
      }
      const txn = dbi_or_txn as Txn;
      if (!!txn.putString) {
        return loadForkDbi(forkId, txn);
      }
      return dbi_or_txn as Dbi;
    })();

    function wrapTxn(txn: ExtendedTxn): ForkTxn;
    function wrapTxn(txn: ExtendedReadonlyTxn): ReadonlyForkTxn;
    function wrapTxn(
      txn: ExtendedTxn | ExtendedReadonlyTxn,
    ): ForkTxn | ReadonlyForkTxn {
      const parentFork = loadParent(txn, forkId);
      const parent = parentFork?.wrapTxn(txn);
      const self = ((txn: ExtendedReadonlyTxn) => {
        const self: ReadonlyForkTxn = {
          txn,
          parent: parentFork,
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
            return JSON.parse(value) as T;
          },
          // similar to 'use database' in mysql, change the implicit dbi
          changeFork(
            forkId_or_fork: number | { forkId: number; dbi: Dbi },
          ): void {
            let forkId: number;
            let dbi: Dbi | undefined;
            if (typeof forkId_or_fork === 'number') {
              forkId = forkId_or_fork;
            } else {
              forkId = forkId_or_fork.forkId;
              dbi = forkId_or_fork.dbi;
            }
            const that = loadFork(
              forkId,
              dbi || ((txn as ReadonlyTxn) as Txn),
            ).wrapTxn(txn);
            Object.assign(self, that);
          },
          forEachKey(f: (key: Key) => void): void {
            const cursor = newCursor(txn as ExtendedTxn, dbi);
            let key = cursor.goToFirst();
            while (key !== null) {
              f(key);
              key = cursor.goToNext();
            }
          },
        };
        return self;
      })(txn);
      if (isReadonly(txn)) {
        return self;
      }
      return ((txn: ExtendedTxn, readonlySelf: ReadonlyForkTxn) => {
        const self: ForkTxn = Object.assign(readonlySelf, {
          txn,
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
          putObject(key: Key, value: any, keyType?: KeyType): void {
            self.putString(key, JSON.stringify(value), keyType);
          },
          del(key: Key, keyType?: KeyType): void {
            txn.del(dbi, key, keyType);
          },
          // cannot inline the impl, otherwise will mark the `fork` under `beginTxn` also deprecated
          fork() {
            const { childID, dbi } = fork(txn, forkId);
            return loadFork(childID, dbi);
          },
          /**
           * if no child, delete directly;
           * if only one child, merge into child;
           * if multiple child, keep the fork, but mark for delete
           * */
          drop(): DropResult {
            // TODO
            const fork = loadForkRecord(txn, forkId);
            if (fork === null) {
              return 'not_exist';
            }
            console.log({
              forkId,
              '# child': fork.childIDs.length,
            });

            function dropFork(fork: { forkId: number; dbi: Dbi }) {
              delForkRecord(txn, fork.forkId);
              fork.dbi.drop({ txn });
            }

            // if no child, delete directly;
            if (fork.childIDs.length === 0) {
              dropFork({ forkId, dbi });
              return 'ok';
            }

            // if only one child, merge into child;
            if (fork.childIDs.length === 1) {
              // TODO
              const res = migrateFork({
                txn,
                selfForkId: forkId,
                selfDbi: dbi,
                childForkId: fork.childIDs[0],
              });
              switch (res) {
                case 'self_to_child':
                case 'child_to_self':
                  return 'ok';
                default:
                  console.error('failed to migrate fork, result:', res);
                  if ((res as any) instanceof Error) {
                    throw res;
                  } else {
                    throw new Error(res);
                  }
              }
            }
            // if multiple child, keep the fork, but mark for delete
            // TODO
            return 'ok';
          },
        });
        return self;
      })(txn as ExtendedTxn, self);
    }

    function beginTxn(options?: { readOnly?: false }): ForkTxn;
    function beginTxn(options: { readOnly: true }): ReadonlyForkTxn;
    function beginTxn(options?: {
      readOnly?: boolean;
    }): ForkTxn | ReadonlyForkTxn {
      const txn =
        options && options.readOnly
          ? env.beginTxn({ readOnly: true })
          : env.beginTxn();
      return wrapTxn(txn);
    }

    return {
      forkId,
      dbi,
      fork(): Fork {
        const txn = env.beginTxn();
        const { childID, dbi } = fork(txn, forkId);
        txn.commit();
        return loadFork(childID, dbi);
      },
      drop(): DropResult {
        const txn = env.beginTxn();
        const result = wrapTxn(txn).drop();
        txn.commit();
        return result;
      },
      wrapTxn,
      beginTxn,
    };
  }

  function loadRoot(forkId: number = 0) {
    const dbi = env.openDbi({
      name: forkIdToKey(forkId),
      create: true,
    });
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
    return loadFork(forkId, dbi);
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

  function dropFork(txn: ExtendedTxn, forkId: number): DropResult {
    let fork: Fork;
    try {
      fork = loadFork(forkId, txn);
    } catch (e) {
      if (
        e instanceof Error &&
        e.message === 'MDB_NOTFOUND: No matching key/data pair found'
      ) {
        return 'not_exist';
      }
      throw e;
    }
    return fork.wrapTxn(txn).drop();
  }

  return {
    fork,
    loadFork,
    loadRoot,
    /**
     * drop the dbi of the given forkId
     * will skip silently if the dbi doesn't exist
     * */
    dropForkWithTxn: dropFork,
    /**
     * @deprecated cannot run within other transaction
     * */
    dropFork(forkId: number): DropResult {
      const txn = env.beginTxn();
      try {
        const res = dropFork(txn, forkId);
        txn.commit();
        return res;
      } catch (e) {
        txn.abort();
        throw e;
      }
    },
    clearAll,
  };
}
