"use strict";

const Message = require("../types/message");
const Wantlist = require("../types/wantlist");
const CONSTANTS = require("../constants");
const MsgQueue = require("./msg-queue");
const logger = require("../utils").logger;

module.exports = class WantManager {
  constructor(peerId, network, stats) {
    this.peers = new Map();
    this.wantlist = new Wantlist(stats);

    this.network = network;
    this._stats = stats;

    this._peerId = peerId;
    this._log = logger(peerId, "want");
    this.busyPeers = new Map();
  }

  getBusyPeers() {
    return Array.from(this.busyPeers.values())
  }

  addPeerBusy(peerId, blockProcessing, failedBlock) {
    this.busyPeers.set(peerId, {peerId, blockProcessing, failedBlock})
    // if(this.busyPeers.has(peerId)){
    //   let element = this.busyPeers.get(peerId)
    //   if(blockProcessing){

    //   }
    // }
  }

  _addEntries(cids, cancel, force) {
    const entries = cids.map((cid, i) => {
      return new Message.Entry(
        cid,
        CONSTANTS.kMaxPriority - i,
        Message.WantType.Block,
        cancel
      );
    });

    entries.forEach((e) => {
      // add changes to our wantlist
      if (e.cancel) {
        if (force) {
          this.wantlist.removeForce(e.cid);
        } else {
          this.wantlist.remove(e.cid);
        }
      } else {
        this._log("adding to wl");
        this.wantlist.add(e.cid, e.priority);
      }
    });

    // broadcast changes
    let $peers = Array.from(this.peers.values());
    let prevEntries = [];
    console.log("all peers", $peers, entries, parseInt(new Date().getTime()/ 1000));
    let availablePeers = $peers.filter((ele) => {
      console.log("ELEMENTSSSSSSS", ele)
      if(!this.busyPeers.has(ele)){
        return ele
      }
    })

    if (availablePeers.length > 0) {
      if (entries.length > 0) {
        console.log("all peers prev", prevEntries, new Date().getTime());

        if (prevEntries.length === 0) {
          this.p = availablePeers[Math.floor(Math.random() * availablePeers.length)];
          console.log("selected peer", this.p, entries, parseInt(new Date().getTime()/ 1000));
          this.p.addEntries(entries);
          this.busyPeers.set(this.p)
        }
        if (
          prevEntries &&
          entries &&
          prevEntries[0] &&
          entries[0] &&
          entries[0].entry.cid.string !== prevEntries[0].entry.cid.string
        ) {
          this.p = $peers[Math.floor(Math.random() * $peers.length)];
          console.log("selected peer", this.p, entries, parseInt(new Date().getTime()/ 1000));
          this.p.addEntries(entries);
        }

        if (this.interval) clearInterval(this.interval);
        this.interval = setInterval(() => {
          if (
            entries[0] &&
            entries[1] &&
            entries[0].entry.cid.string === entries[1].entry.cid.string &&
            !entries[0].cancel &&
            !entries[1].cancel
          ) {
            console.log("entries first condition", entries, parseInt(new Date().getTime()/ 1000));
            this.p = $peers[Math.floor(Math.random() * $peers.length)];
            console.log("selected peer", this.p, entries, parseInt(new Date().getTime()/ 1000));
            this.p.addEntries(entries);
          }
          if (
            prevEntries.length === 1 &&
            entries.length === 1 &&
            prevEntries[0] &&
            entries[0] &&
            entries[0].entry.cid.string === prevEntries[0].entry.cid.string &&
            !entries[0].cancel
          ) {
            // clearTimeout(interval);
            console.log("entries second condition", entries, prevEntries, parseInt(new Date().getTime()/ 1000));
            // this.p = $peers[Math.floor(Math.random() * $peers.length)];
            this.p = availablePeers[Math.floor(Math.random() * availablePeers.length)];

            console.log("selected peer", this.p, entries, parseInt(new Date().getTime()/ 1000));
            this.p.addEntries(entries);
            // this.p.
            this.busyPeers.set(this.p)

          }
        }, 4000);
      }
    }

    if (entries) {
      prevEntries = [...entries];
    }

    // let i= 0;
    // for (const p of this.peers.values()) {
    //   if(i<2){
    //   console.log('add entrries to peer',p);
    //   p.addEntries(entries)
    //   }
    //   i++;
    // }
  }

  _startPeerHandler(peerId) {
    let mq = this.peers.get(peerId.toB58String());

    if (mq) {
      mq.refcnt++;
      return;
    }

    mq = new MsgQueue(this._peerId, peerId, this.network);

    // new peer, give them the full wantlist
    const fullwantlist = new Message(true);

    for (const entry of this.wantlist.entries()) {
      fullwantlist.addEntry(entry[1].cid, entry[1].priority);
    }

    mq.addMessage(fullwantlist);

    this.peers.set(peerId.toB58String(), mq);
    return mq;
  }

  _stopPeerHandler(peerId) {
    const mq = this.peers.get(peerId.toB58String());

    if (!mq) {
      return;
    }

    mq.refcnt--;
    if (mq.refcnt > 0) {
      return;
    }

    this.peers.delete(peerId.toB58String());
  }

  // add all the cids to the wantlist
  wantBlocks(cids, options = {}) {
    this._addEntries(cids, false);

    if (options && options.signal) {
      options.signal.addEventListener("abort", () => {
        this.cancelWants(cids);
      });
    }
  }

  // remove blocks of all the given keys without respecting refcounts
  unwantBlocks(cids) {
    this._log("unwant blocks: %s", cids.length);
    this._addEntries(cids, true, true);
  }

  // cancel wanting all of the given keys
  cancelWants(cids) {
    this._log("cancel wants: %s", cids.length);
    this._addEntries(cids, true);
  }

  // Returns a list of all currently connected peers
  connectedPeers() {
    return Array.from(this.peers.keys());
  }

  connected(peerId) {
    this._startPeerHandler(peerId);
  }

  disconnected(peerId) {
    this._stopPeerHandler(peerId);
  }

  start() {}

  stop() {
    this.peers.forEach((mq) => this.disconnected(mq.peerId));

    clearInterval(this.timer);
  }
};
