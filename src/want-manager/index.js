"use strict";

const Message = require("../types/message");
const Wantlist = require("../types/wantlist");
const CONSTANTS = require("../constants");
const MsgQueue = require("./msg-queue");
const logger = require("../utils").logger;
let retryTime = 3
module.exports = class WantManager {
  constructor(peerId, network, stats) {
    this.peers = new Map();
    this.wantlist = new Wantlist(stats);

    this.network = network;
    this._stats = stats;

    this._peerId = peerId;
    this._log = logger(peerId, "want");
    this.busyPeers = new Map();
    this.availablePeers = [];
    this.sentRequests = new Map();
    this.prevEntries = [];
    this.storedCIDS = [];
    this.root = false;
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

  removeBusyPeer(peerId,blocks) {
    console.log("INSIDE BUSY PEERS REMOVE",blocks, peerId.toB58String(), this.busyPeers, this.availablePeers)
    if(this.busyPeers.has(peerId.toB58String())) {    
      let receivedBlockFrom = this.busyPeers.get(peerId.toB58String());
      this.busyPeers.delete(peerId.toB58String())
      this.availablePeers.push(receivedBlockFrom.peer)
      console.log("INSIDE BUSY PEERS REMOVE", this.busyPeers, this.availablePeers)
    }else {
      console.log("INSIDE REMOVE, PEER WAS NOT IN BUSY ARRAY")
    }
    // let $peers = Array.from(this.peers.values());

    // this.availablePeers = $peers.filter((ele) => {

    //   if(this.busyPeers.has(ele.peerId.toB58String())){
    //     let bp = this.busyPeers.get(ele.peerId.toB58String())
    //     if(parseInt(new Date().getTime()/1000) >= bp.addedAt + retryTime ){
    //       this.busyPeers.delete(ele.peerId.toB58String())
    //       return ele;
    //     }
    //   }
    //   // console.log("ELEMENTSSSSSSS", ele)
    //   else {
    //     return ele
    //   }
    // })
  }

  _addEntries(cids, cancel, force) {
    console.log("ADD ENTRIES CIDD LENGHT", cids.length)
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
    console.log("all peers", $peers, entries, parseInt(new Date().getTime()/ 1000));

    this.p = $peers[Math.floor(Math.random() * $peers.length)];
    console.log("THIS.P", this.p)
    this.p.addEntries(entries)
    // if(this.availablePeers.length === 0){
    //   this.availablePeers = [...$peers];
    //   this.busyPeers = new Map();
    // }

    // if (this.availablePeers.length > 0) {
    //   if (entries.length > 0) {
    //     console.log('already asent request',!this.sentRequests.has(entries[0].entry.cid.string),JSON.stringify(entries),this.sentRequests,entries[0].entry.cid.string)
    //     if(entries[0].entry.cid.string && this.sentRequests.has(entries[0].entry.cid.string) && entries[0].cancel ) {
    //       let requestSentToPeers = this.sentRequests.get(entries[0].entry.cid.string)
    //       console.log("selected peer cancel", requestSentToPeers.peers, entries, parseInt(new Date().getTime()/ 1000));

    //       for(let i of requestSentToPeers.peers){
    //         i.addEntries(entries)
    //       }

    //       this.sentRequests.delete(entries[0].entry.cid.string)
    //     }
    //     else if(entries[0].entry.cid.string && !this.sentRequests.has(entries[0].entry.cid.string)){
    //       console.log("AVAILABLE PEERS", this.availablePeers, this.busyPeers, parseInt(new Date().getTime()/ 1000));
    //       let tempPeer;
         
    //       this.p = this.availablePeers.shift();
    //       this.sentRequests.set(entries[0].entry.cid.string, {peers: [this.p]})
    //       console.log("selected peer", this.p, entries, parseInt(new Date().getTime()/ 1000));
    //       this.p.addEntries(entries);
    //       if(this.busyPeers.keys().length > 0){
    //         tempPeer = this.busyPeers.get(this.busyPeers.keys()[0]);
    //         this.busyPeers.delete(this.busyPeers.keys()[0]);
    //        }
    //       this.busyPeers.set(this.p.peerId.toB58String(), {blockProcessing: true, addedAt: parseInt(new Date().getTime()/ 1000), peer: this.p});
    //       if(tempPeer) this.availablePeers.push(tempPeer.peer);

    //     } 
          
    //     else{
    //       console.log('no condition satisfied');
    //     }
    //     // }
    //     // if (
    //     //   this.prevEntries &&
    //     //   entries &&
    //     //   this.prevEntries[0] &&
    //     //   entries[0] &&
    //     //   entries[0].entry.cid.string !== this.prevEntries[0].entry.cid.string
    //     // ) {
          
    //     //   console.log("AVAILABLE PEERS", this.availablePeers, this.busyPeers, parseInt(new Date().getTime()/ 1000));

    //     //   this.p = this.availablePeers[Math.floor(Math.random() * this.availablePeers.length)];
    //     //   console.log("selected peer", this.p, entries, parseInt(new Date().getTime()/ 1000));
    //     //   this.p.addEntries(entries);
    //     //   this.busyPeers.set(this.p.peerId.toB58String(), {blockProcessing: true, addedAt: parseInt(new Date().getTime()/ 1000)})
    //     // }

    //     if (this.interval) clearInterval(this.interval);
    //     this.interval = setInterval(() => {
    //       if(this.availablePeers.length === 0){
    //         console.log('return due to no available peers');
    //         return
    //       }

          
    //       if(entries[0].entry.cid.string && this.sentRequests.has(entries[0].entry.cid.string)) {
    //         if (
    //           this.prevEntries.length === 1 &&
    //           entries.length === 1 &&
    //           this.prevEntries[0] &&
    //           entries[0] &&
    //           entries[0].entry.cid.string === this.prevEntries[0].entry.cid.string &&
    //           !entries[0].cancel
    //         ) {
              
    //           let requestSentTo = this.sentRequests.get(entries[0].entry.cid.string)
    //           console.log("AVAILABLE PEERS", this.availablePeers, this.busyPeers, "requestSentTo", requestSentTo, parseInt(new Date().getTime()/ 1000));
  
    //           // clearTimeout(interval);
    //           // this.p = $peers[Math.floor(Math.random() * $peers.length)];
    //           let rPeers = this.availablePeers.filter((ele) => !requestSentTo.peers.includes(ele))
    //           console.log("entries second condition", entries, this.prevEntries, rPeers, parseInt(new Date().getTime()/ 1000));

    //          if(rPeers && rPeers.length > 0) { 
    //           this.p = rPeers.shift();
  
    //           console.log("selected peer", this.p, entries, parseInt(new Date().getTime()/ 1000));
    //           this.p.addEntries(entries);
    //           // this.p.
    //           this.busyPeers.set(this.p.peerId.toB58String(), {blockProcessing: true, addedAt: parseInt(new Date().getTime()/ 1000), peer: this.p});
    //           this.sentRequests.set(entries[0].entry.cid.string, {peers: [...requestSentTo.peers, this.p]})
    //         }
    //         else{
    //           console.log('rpeers not defined or blank array')
    //         }
    //         }
    //       }

    //       else{

    //       console.log('wasted one request');  

    //       }
    //     }, retryTime * 1000);
    //   }
    // }

    // if (entries) {
    //   this.prevEntries = [...entries];
    // }

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
    this.storedCIDS.push(cids)
    console.log("INSIDE WANT BLOCK", cids)
    if(this.root && this.storedCIDS.length === 2) {
      this._addEntries(this.storedCIDS, false);
      this.storedCIDS = []
    }
    if(!this.root){
      this._addEntries(cids, false);
      this.root = true;
      this.storedCIDS = []
    }

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
